//! WebSocket handler using broadcast channels
//!
//! Supports multiple connections per container.
//! Never blocks - all updates come via event hub.
//! Uses lock-free state manager for status queries.

use axum::extract::ws::{Message, WebSocket};
use bollard::container::{LogOutput, LogsOptions, StatsOptions, AttachContainerOptions};
use bollard::system::EventsOptions;
use bollard::exec::{CreateExecOptions, StartExecResults};
use chrono::{DateTime, FixedOffset, Utc};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn, error};

use crate::types::AppState;
use crate::services::{ContainerEvent, ContainerEventHub, EventContainerStats};
use super::{WebSocketToken, WsMessage, WsMode};

async fn is_container_running(docker: &bollard::Docker, container_id: &str) -> bool {
    match tokio::time::timeout(Duration::from_secs(2), docker.inspect_container(container_id, None)).await {
        Ok(Ok(info)) => info.state.and_then(|s| s.running).unwrap_or(false),
        _ => false,
    }
}

fn parse_docker_timestamp_prefix(line: &str) -> (Option<i64>, &str) {
    let Some((ts, rest)) = line.split_once(' ') else {
        return (None, line);
    };

    match DateTime::<FixedOffset>::parse_from_rfc3339(ts) {
        Ok(dt) => (Some(dt.timestamp()), rest),
        Err(_) => (None, line),
    }
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn get_container_started_at(docker: &bollard::Docker, container_id: &str) -> Option<i64> {
    docker
        .inspect_container(container_id, None)
        .await
        .ok()
        .and_then(|info| info.state)
        .and_then(|s| s.started_at)
        .and_then(|started| DateTime::parse_from_rfc3339(&started).ok())
        .map(|dt| dt.timestamp())
}

fn docker_action_to_state(action: &str) -> Option<&'static str> {
    match action {
        "unpause" => Some("running"),
        "die" | "stop" | "kill" | "oom" | "exited" => Some("stopped"),
        "restart" => Some("restarting"),
        "pause" => Some("paused"),
        _ => None,
    }
}

fn is_streaming_state(state: &str) -> bool {
    matches!(
        state,
        "running" | "starting" | "installing" | "updating" | "recreating" | "restoring" | "creating"
    )
}


pub struct WebSocketHandler {
    token: WebSocketToken,
    socket: WebSocket,
    state: Arc<AppState>,
    mode: WsMode,
}

impl WebSocketHandler {
    pub fn new(token: WebSocketToken, socket: WebSocket, state: Arc<AppState>) -> Self {
        Self { 
            token, 
            socket, 
            state,
            mode: WsMode::default(), // Default to attached mode
        }
    }

    pub async fn handle(mut self) {
        let container_id = self.token.container_id.clone();
        let container_uuid = self.token.container_uuid.clone();
        let docker = Arc::new(self.state.docker.client.clone());
        let event_hub = self.state.event_hub.clone();

        info!("WebSocket opened for container: {} ({}) in {:?} mode", container_uuid, container_id, self.mode);

        let (mut ws_tx, mut ws_rx) = self.socket.split();

        // Subscribe to container events
        let mut event_rx = event_hub.subscribe(&container_id).await;

        // Get current status directly from Docker (no lock needed)
        let current_status = Self::get_container_status(&self.state, &docker, &container_id, &container_uuid).await;
        let is_running = current_status == "running";
        let is_streaming = is_streaming_state(&current_status);
        let mut current_state = current_status.clone();

        // Send init message with mode info
        let init_msg = WsMessage::init(&container_id, &container_uuid, &current_status);
        if ws_tx.send(Message::Text(init_msg.to_json())).await.is_err() {
            warn!("Failed to send init message");
            event_hub.unsubscribe(&container_id).await;
            return;
        }
        
        // Notify client of current mode
        let mode_msg = WsMessage::mode_changed(match self.mode {
            WsMode::Attached => "attached",
            WsMode::Exec => "exec",
        });
        let _ = ws_tx.send(Message::Text(mode_msg.to_json())).await;

        // Start streaming if running or starting
        let (log_task, attached_input_tx) = if is_streaming {
            let (task, input_tx) = Self::spawn_log_streamer(docker.clone(), container_id.clone(), event_hub.clone(), self.mode);
            (Some(task), input_tx)
        } else {
            (None, None)
        };

        let stats_task = if is_streaming {
            Some(Self::spawn_stats_streamer(docker.clone(), container_id.clone(), event_hub.clone()))
        } else {
            None
        };

        let mut container_running = is_running;
        let mut last_start_since: Option<i64> = None;
        let mut send_startup_tail = false;
        let mut container_streaming = is_streaming;
        let mut current_log_task: Option<tokio::task::JoinHandle<()>> = log_task;
        let mut attached_input_tx: Option<mpsc::UnboundedSender<String>> = attached_input_tx;
        let mut current_stats_task: Option<tokio::task::JoinHandle<()>> = stats_task;
        let mut last_sm_state = current_status.clone();
        let mut last_lock_reason: Option<String> = None;
        let mut last_operation: Option<String> = None;
        let mut last_operation_id: Option<String> = None;
        let mut last_operation_message: Option<String> = None;

        let (docker_event_tx, mut docker_event_rx) = mpsc::unbounded_channel::<(String, String)>();
        let docker_event_task = Self::spawn_docker_event_streamer(
            docker.clone(),
            container_id.clone(),
            event_hub.clone(),
            docker_event_tx,
        );

        if !is_streaming_state(&current_status) {
            let msg = WsMessage::daemon_message("Server is not running, start it!").to_json();
            let _ = ws_tx.send(Message::Text(msg)).await;
        }

        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    match event {
                        Ok(ContainerEvent::StateChanged { state: docker_state }) => {
                            // During operations (installing, recreating, etc.), Docker events like "die" 
                            // should not override the operation state. Use get_container_status which
                            // respects state manager for transitional states.
                            let state = if docker_state == "stopped" || docker_state == "exited" {
                                // Check if there's an active operation - if so, don't send "stopped"
                                let actual_state = Self::get_container_status(&self.state, &docker, &container_id, &container_uuid).await;
                                if matches!(actual_state.as_str(), "installing" | "updating" | "recreating" | "restoring" | "creating") {
                                    // Don't send status during operation - operation will send final status
                                    debug!("Suppressing Docker '{}' event during '{}' operation", docker_state, actual_state);
                                    continue;
                                }
                                actual_state
                            } else {
                                docker_state.clone()
                            };

                            let was_running = container_running;
                            let was_streaming = container_streaming;
                            container_running = state == "running";
                            container_streaming = is_streaming_state(&state);
                            current_state = state.clone();

                            if state == "stopped" || state == "exited" {
                                last_start_since = None;
                                send_startup_tail = false;
                            }

                            // If container stopped, get exit code and send crash info (but not during operations)
                            if was_running && !container_running && (state == "exited" || state == "stopped") {
                                // Get container inspect to check exit code
                                if let Ok(inspect) = docker.inspect_container(&container_id, None).await {
                                    if let Some(container_state) = inspect.state {
                                        let exit_code = container_state.exit_code.unwrap_or(0);
                                        
                                        // Send exit notification with code
                                        let exit_msg = if exit_code != 0 {
                                            let error_msg = container_state.error.unwrap_or_default();
                                            if !error_msg.is_empty() {
                                                format!("Container exited with code {} - {}", exit_code, error_msg)
                                            } else {
                                                format!("Container exited with code {}", exit_code)
                                            }
                                        } else {
                                            "Container exited normally".to_string()
                                        };
                                        
                                        // Log to Lightd logs
                                        if exit_code != 0 {
                                            warn!("Container {} crashed: {}", container_id, exit_msg);
                                        } else {
                                            info!("Container {} exited normally", container_id);
                                        }
                                        
                                        // Send exit notification to WebSocket
                                        let msg = WsMessage::daemon_message(&exit_msg).to_json();
                                        let _ = ws_tx.send(Message::Text(msg)).await;
                                        
                                        // Send explicit message to trigger frontend reconnect
                                        let reconnect_msg = WsMessage::daemon_message("WebSocket will close - reconnect to see new logs").to_json();
                                        let _ = ws_tx.send(Message::Text(reconnect_msg)).await;
                                    }
                                }
                            }

                            let msg = WsMessage::status(&state).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() {
                                break;
                            }

                            if container_streaming && !was_streaming {
                                let (task, input_tx) = Self::spawn_log_streamer(
                                    docker.clone(), container_id.clone(), event_hub.clone(), self.mode,
                                );
                                current_log_task = Some(task);
                                attached_input_tx = input_tx;
                                current_stats_task = Some(Self::spawn_stats_streamer(
                                    docker.clone(), container_id.clone(), event_hub.clone(),
                                ));
                            } else if !container_streaming && was_streaming {
                                if let Some(task) = current_log_task.take() { task.abort(); }
                                attached_input_tx = None;
                                if let Some(task) = current_stats_task.take() { task.abort(); }
                            }

                            if container_running && !was_running {
                                if send_startup_tail {
                                    let since = get_container_started_at(&docker, &container_id).await
                                        .or(last_start_since)
                                        .unwrap_or_else(now_epoch_secs);
                                    last_start_since = Some(since);
                                    if let Err(e) = Self::send_initial_logs(&docker, &container_id, &mut ws_tx, "200", Some(since)).await {
                                        debug!("Failed to send startup logs for {}: {}", container_id, e);
                                    }
                                    send_startup_tail = false;
                                }
                            } else if !container_running && was_running {
                                // Stats streamer is already handled by container_streaming transitions.
                            }
                        }
                        Ok(ContainerEvent::ConsoleOutput { line }) => {
                            let msg = WsMessage::console_output(&line).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Ok(ContainerEvent::ConsoleDuplicate { count }) => {
                            let msg = WsMessage::console_duplicate(count).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Ok(ContainerEvent::Stats(stats)) => {
                            let msg = WsMessage::stats(
                                stats.memory_bytes, stats.memory_limit_bytes, stats.cpu_percent,
                                stats.network_rx_bytes, stats.network_tx_bytes, stats.uptime,
                                &current_state,
                                stats.disk_bytes,
                            ).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Ok(ContainerEvent::DaemonMessage { message }) => {
                            let msg = WsMessage::daemon_message(&message).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Ok(ContainerEvent::PowerActionStarted { action }) => {
                            let msg = WsMessage::daemon_message(&format!("Power action started: {}", action)).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }

                            if action == "start" || action == "restart" {
                                last_start_since = Some(now_epoch_secs());
                                send_startup_tail = true;
                            }
                            if action == "stop" || action == "kill" {
                                last_start_since = None;
                                send_startup_tail = false;
                            }
                        }
                        Ok(ContainerEvent::PowerActionCompleted { action, success, message }) => {
                            let status = if success { "completed" } else { "failed" };
                            let msg = WsMessage::daemon_message(&format!("Power action {}: {} - {}", action, status, message)).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            debug!("WebSocket lagged {} messages for {}", n, container_id);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Event channel closed for {}", container_id);
                            break;
                        }
                    }
                }

                docker_event = docker_event_rx.recv() => {
                    if let Some((action, state)) = docker_event {
                        let msg = WsMessage::docker_event(&action, &state).to_json();
                        if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                    }
                }

                msg = ws_rx.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(client_msg) = serde_json::from_str::<super::WsClientMessage>(&text) {
                                match client_msg {
                                    super::WsClientMessage::SendCommand(args) if !args.is_empty() => {
                                        let cmd = &args[0];
                                        debug!("Executing command in {} ({:?} mode): {}", container_id, self.mode, cmd);

                                        match self.mode {
                                            WsMode::Attached => {
                                                if let Some(tx) = attached_input_tx.as_ref() {
                                                    if tx.send(cmd.clone()).is_err() {
                                                        let _ = ws_tx.send(Message::Text(
                                                            WsMessage::error("Failed to send command to attached session").to_json()
                                                        )).await;
                                                    }
                                                } else {
                                                    let _ = ws_tx.send(Message::Text(
                                                        WsMessage::error("No attached session available").to_json()
                                                    )).await;
                                                }
                                            }
                                            WsMode::Exec => {
                                                let docker_clone = docker.clone();
                                                let cid = container_id.clone();
                                                let hub = event_hub.clone();
                                                let command = cmd.clone();
                                                
                                                tokio::spawn(async move {
                                                    Self::execute_command(&docker_clone, &cid, &command, &hub, WsMode::Exec).await;
                                                });
                                            }
                                        }
                                    }
                                    super::WsClientMessage::Power(args) if !args.is_empty() => {
                                        let action = args[0].to_lowercase();
                                        let state = self.state.clone();
                                        let cid = container_id.clone();
                                        let uuid = container_uuid.clone();
                                        let hub = event_hub.clone();

                                        tokio::spawn(async move {
                                            if let Ok(Some(tracker)) = state.container_tracker.get_container(&uuid).await {
                                                if let Some(runtime) = tracker.runtime {
                                                    state.async_power.set_runtime(&cid, &runtime.start_up, &runtime.stop).await;
                                                }
                                            }

                                            let result = match action.as_str() {
                                                "start" => state.async_power.start(cid.clone(), uuid.clone()).await,
                                                "stop" => state.async_power.stop(cid.clone(), uuid.clone()).await,
                                                "restart" => state.async_power.restart(cid.clone(), uuid.clone()).await,
                                                "kill" => state.async_power.kill(cid.clone(), uuid.clone()).await,
                                                _ => Err("Invalid power action".to_string()),
                                            };

                                            if let Err(err) = result {
                                                hub.broadcast_message(&cid, &format!("Power action failed: {}", err)).await;
                                            }
                                        });
                                    }
                                    super::WsClientMessage::SetMode(args) if !args.is_empty() => {
                                        let new_mode = match args[0].to_lowercase().as_str() {
                                            "attached" => WsMode::Attached,
                                            "exec" => WsMode::Exec,
                                            _ => {
                                                let _ = ws_tx.send(Message::Text(
                                                    WsMessage::error("Invalid mode. Use 'attached' or 'exec'").to_json()
                                                )).await;
                                                continue;
                                            }
                                        };
                                        
                                        if new_mode != self.mode {
                                            info!("Switching WebSocket mode from {:?} to {:?} for {}", self.mode, new_mode, container_id);
                                            self.mode = new_mode;
                                            
                                            // Restart log streamer with new mode
                                            if container_streaming {
                                                if let Some(task) = current_log_task.take() { 
                                                    task.abort(); 
                                                }
                                                let (task, input_tx) = Self::spawn_log_streamer(
                                                    docker.clone(), container_id.clone(), event_hub.clone(), self.mode,
                                                );
                                                current_log_task = Some(task);
                                                attached_input_tx = input_tx;
                                            } else {
                                                attached_input_tx = None;
                                            }
                                            
                                            let mode_str = match new_mode {
                                                WsMode::Attached => "attached",
                                                WsMode::Exec => "exec",
                                            };
                                            let _ = ws_tx.send(Message::Text(
                                                WsMessage::mode_changed(mode_str).to_json()
                                            )).await;
                                        }
                                    }
                                    super::WsClientMessage::RequestLogs(args) => {
                                        let tail = args.get(0).cloned().unwrap_or_else(|| "10".to_string());
                                        let since = if let Some(since) = last_start_since {
                                            Some(since)
                                        } else {
                                            get_container_started_at(&docker, &container_id).await
                                        };
                                        if let Err(e) = Self::send_initial_logs(&docker, &container_id, &mut ws_tx, &tail, since).await {
                                            debug!("Failed to send requested logs for {}: {}", container_id, e);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            if ws_tx.send(Message::Pong(data)).await.is_err() { break; }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            debug!("Client disconnected");
                            break;
                        }
                        Some(Err(e)) => {
                            warn!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }

                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if let Some(container_state) = self.state.state_manager.get_container(&container_uuid) {
                        let actual_state = Self::get_container_status(&self.state, &docker, &container_id, &container_uuid).await;
                        if actual_state != last_sm_state {
                            last_sm_state = actual_state.clone();
                            current_state = actual_state.clone();
                            let msg = WsMessage::status(&actual_state).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }

                        let lock_reason = container_state.lock_reason.clone();
                        let operation = container_state.operation.clone();
                        let operation_id = container_state.operation_id.clone();
                        let operation_message = container_state.operation_message.clone();

                        if lock_reason != last_lock_reason
                            || operation != last_operation
                            || operation_id != last_operation_id
                            || operation_message != last_operation_message
                        {
                            last_lock_reason = lock_reason.clone();
                            last_operation = operation.clone();
                            last_operation_id = operation_id.clone();
                            last_operation_message = operation_message.clone();

                            let detail = operation_message.or(lock_reason);
                            if let Some(detail) = detail {
                                let mut msg = String::new();
                                if let Some(op) = operation.as_deref() {
                                    msg.push_str(&format!("{} - {}", op, detail));
                                } else {
                                    msg.push_str(&format!("{}", detail));
                                }

                                if let Some(op_id) = operation_id.as_deref() {
                                    // Operation:
                                    //msg.push_str(&format!(" (id: {})", op_id));
                                }

                                let ws_msg = WsMessage::daemon_message(&msg).to_json();
                                if ws_tx.send(Message::Text(ws_msg)).await.is_err() { break; }
                            }
                        }
                    }
                }

                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if ws_tx.send(Message::Ping(vec![])).await.is_err() { break; }
                }
            }
        }

        if let Some(task) = current_log_task { task.abort(); }
        if let Some(task) = current_stats_task { task.abort(); }
        docker_event_task.abort();
        event_hub.unsubscribe(&container_id).await;
        info!("WebSocket closed for container: {}", container_id);
    }

    /// Get container status directly from Docker (no state manager lock)
    async fn get_container_status(
        state: &AppState,
        docker: &bollard::Docker,
        container_id: &str,
        container_uuid: &str,
    ) -> String {
        // Prefer state manager for transitional states
        // Prefer async power pending actions for transitional states so we don't jump to running too early.
        if let Some(action) = state.async_power.has_pending_action(container_id).await {
            if action == "start" || action == "restart" {
                return "starting".to_string();
            }
            if action == "stop" || action == "kill" {
                return "stopping".to_string();
            }
        }

        // Fall back to state manager for transitional states.
        if let Some((_uuid, container_state)) = state.state_manager.find_by_container_id(container_id) {
            let status = container_state.state.clone();
            if matches!(
                status.as_str(),
                "starting" | "stopping" | "installing" | "updating" | "recreating" | "restoring" | "creating" | "suspended" | "locked" | "install_failed" | "update_failed" | "install_timeout" | "update_timeout" | "install_success"
            ) {
                return status;
            }
        } else if let Some(container_state) = state.state_manager.get_container(container_uuid) {
            let status = container_state.state.clone();
            // If the container_id changed (reinstall), prefer the state manager status
            // since docker inspect on the old container_id may fail and return "offline".
            let id_mismatch = container_state.container_id.as_deref() != Some(container_id);
            if id_mismatch {
                return status;
            }
            if matches!(
                status.as_str(),
                "starting" | "stopping" | "installing" | "updating" | "recreating" | "restoring" | "creating" | "suspended" | "locked" | "install_failed" | "update_failed" | "install_timeout" | "update_timeout" | "install_success"
            ) {
                return status;
            }
        }

        match tokio::time::timeout(
            Duration::from_secs(2),
            docker.inspect_container(container_id, None)
        ).await {
            Ok(Ok(info)) => {
                if let Some(state) = info.state {
                    if state.running == Some(true) { "running".to_string() }
                    else if state.paused == Some(true) { "paused".to_string() }
                    else if state.restarting == Some(true) { "restarting".to_string() }
                    else { "stopped".to_string() }
                } else {
                    "unknown".to_string()
                }
            }
            Ok(Err(_)) => "offline".to_string(),
            Err(_) => "timeout".to_string(),
        }
    }

    fn spawn_log_streamer(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
        mode: WsMode,
    ) -> (tokio::task::JoinHandle<()>, Option<mpsc::UnboundedSender<String>>) {
        match mode {
            WsMode::Attached => {
                let (tx, rx) = mpsc::unbounded_channel();
                let task = tokio::spawn(async move {
                    Self::stream_logs_attached(docker, container_id, event_hub, rx).await;
                });
                (task, Some(tx))
            }
            WsMode::Exec => {
                let task = tokio::spawn(async move {
                    Self::stream_logs_exec(docker, container_id, event_hub).await;
                });
                (task, None)
            }
        }
    }
    
    /// Stream logs in attached mode - uses docker attach for stdin + docker logs for output
    /// This approach handles containers that exit quickly by using logs API with follow=true
    async fn stream_logs_attached(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
        mut input_rx: mpsc::UnboundedReceiver<String>,
    ) {
        let mut last_line: Option<String> = None;
        let mut duplicate_count: u32 = 0;

        info!("[LOG_STREAM] Starting log streamer for container {}", container_id);

        // Spawn a task for stdin handling (attach for input only)
        let docker_input = docker.clone();
        let cid_input = container_id.clone();
        let _stdin_task = tokio::spawn(async move {
            loop {
                // Wait for container to be running before attaching for stdin
                if !is_container_running(&docker_input, &cid_input).await {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                let attach_opts = AttachContainerOptions::<String> {
                    stdin: Some(true),
                    stdout: Some(false),
                    stderr: Some(false),
                    stream: Some(true),
                    logs: Some(false),
                    ..Default::default()
                };

                match docker_input.attach_container(&cid_input, Some(attach_opts)).await {
                    Ok(attached) => {
                        info!("[LOG_STREAM] Attached stdin to container {}", cid_input);
                        let mut input = attached.input;
                        
                        while let Some(command) = input_rx.recv().await {
                            info!("[LOG_STREAM] Sending command to container {}: {}", cid_input, command);
                            let payload = format!("{}\n", command);
                            if let Err(e) = input.write_all(payload.as_bytes()).await {
                                error!("[LOG_STREAM] Failed to write to stdin for {}: {}", cid_input, e);
                                break;
                            }
                            let _ = input.flush().await;
                        }
                    }
                    Err(e) => {
                        debug!("[LOG_STREAM] Failed to attach stdin to {}: {}", cid_input, e);
                    }
                }
                
                // If we get here, either attach failed or container stopped
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        // Main log streaming loop using docker logs API with follow=true
        let mut backoff = Duration::from_millis(100);
        let mut log_count: u64 = 0;

        loop {
            // Check if container exists and is running
            let running = is_container_running(&docker, &container_id).await;
            if !running {
                debug!("[LOG_STREAM] Container {} not running, waiting...", container_id);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }

            info!("[LOG_STREAM] Container {} is running, starting log stream (follow=true)", container_id);
            backoff = Duration::from_millis(100);

            // Get container start time for filtering
            let since = get_container_started_at(&docker, &container_id).await.unwrap_or(0);

            let log_opts = LogsOptions::<String> {
                follow: true,
                stdout: true,
                stderr: true,
                since,
                timestamps: false,
                tail: "0".to_string(), // Don't replay old logs, just stream new ones
                ..Default::default()
            };

            let mut log_stream = docker.logs(&container_id, Some(log_opts));

            while let Some(result) = log_stream.next().await {
                match result {
                    Ok(log_output) => {
                        let message_bytes = match log_output {
                            LogOutput::StdOut { message } |
                            LogOutput::StdErr { message } |
                            LogOutput::Console { message } |
                            LogOutput::StdIn { message } => message,
                        };

                        let message = String::from_utf8_lossy(&message_bytes);
                        for line in message.lines() {
                            let line = line.trim_end();
                            if !line.is_empty() {
                                log_count += 1;
                                info!("[LOG_STREAM] Container {} log #{}: {}", container_id, log_count, line);
                                
                                if let Some(ref last) = last_line {
                                    if last == line {
                                        duplicate_count += 1;
                                        event_hub.broadcast_console_duplicate(&container_id, duplicate_count).await;
                                        continue;
                                    }
                                }

                                last_line = Some(line.to_string());
                                duplicate_count = 1;
                                event_hub.broadcast_console(&container_id, line).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("[LOG_STREAM] Log stream error for {}: {}", container_id, e);
                        break;
                    }
                }
            }

            info!("[LOG_STREAM] Log stream ended for {} (total {} logs)", container_id, log_count);
            
            // Small delay before retry
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

    }

    async fn send_initial_logs(
        docker: &bollard::Docker,
        container_id: &str,
        ws_tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
        tail: &str,
        since: Option<i64>,
    ) -> Result<(), String> {
        let opts = LogsOptions::<String> {
            follow: false,
            stdout: true,
            stderr: true,
            since: since.unwrap_or(0),
            timestamps: true,
            tail: tail.to_string(),
            ..Default::default()
        };

        let mut stream = docker.logs(container_id, Some(opts));
        while let Some(result) = stream.next().await {
            match result {
                Ok(log_output) => {
                    let message_bytes = match log_output {
                        LogOutput::StdOut { message } |
                        LogOutput::StdErr { message } |
                        LogOutput::Console { message } |
                        LogOutput::StdIn { message } => message,
                    };
                    let message = String::from_utf8_lossy(&message_bytes);
                    for line in message.lines() {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        let (_, content) = parse_docker_timestamp_prefix(line);
                        let content = content.trim();
                        if content.is_empty() {
                            continue;
                        }
                        let msg = WsMessage::console_output(content).to_json();
                        if ws_tx.send(Message::Text(msg)).await.is_err() {
                            return Err("client disconnected".to_string());
                        }
                    }
                }
                Err(e) => {
                    return Err(e.to_string());
                }
            }
        }

        Ok(())
    }

    fn spawn_docker_event_streamer(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
        tx: mpsc::UnboundedSender<(String, String)>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(250);

            loop {
                let mut filters = HashMap::new();
                filters.insert("type".to_string(), vec!["container".to_string()]);
                filters.insert("container".to_string(), vec![container_id.clone()]);

                let opts = EventsOptions::<String> {
                    since: None,
                    until: None,
                    filters,
                };

                let mut stream = docker.events(Some(opts));

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(event) => {
                            let raw_action = event.action
                                .clone()
                                .unwrap_or_default();

                            if raw_action.is_empty() {
                                continue;
                            }

                            let action = raw_action.split(':').next().unwrap_or("").trim().to_string();
                            if action.is_empty() {
                                continue;
                            }

                            // Only forward state-changing events to clients; ignore noisy exec/health events.
                            if action.starts_with("exec_") || action.starts_with("health_status") {
                                continue;
                            }

                            let state = docker_action_to_state(&action)
                                .unwrap_or("")
                                .to_string();

                            if !state.is_empty() {
                                event_hub.broadcast_state(&container_id, &state).await;
                            }

                            let should_forward = matches!(
                                action.as_str(),
                                "start" | "die" | "stop" | "kill" | "restart" | "pause" | "unpause" | "oom" | "destroy"
                            );

                            if should_forward {
                                let _ = tx.send((action, state));
                            }

                            backoff = Duration::from_millis(250);
                        }
                        Err(e) => {
                            debug!("Docker events stream error for {}: {}", container_id, e);
                            break;
                        }
                    }
                }

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        })
    }
    
    /// Stream logs in exec mode - uses docker attach (logs + live)
    async fn stream_logs_exec(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
    ) {
        info!("Starting exec mode log streamer for {}", container_id);

        let mut backoff = Duration::from_millis(250);
        let mut last_line: Option<String> = None;
        let mut duplicate_count: u32 = 0;

        loop {
            if !is_container_running(&docker, &container_id).await {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let attach_opts = AttachContainerOptions::<String> {
                stdin: Some(false),
                stdout: Some(true),
                stderr: Some(true),
                stream: Some(true),
                logs: Some(false),
                ..Default::default()
            };

            match docker.attach_container(&container_id, Some(attach_opts)).await {
                Ok(attached) => {
                    info!("Attached to container {} in exec mode", container_id);
                    backoff = Duration::from_millis(250);

                    let mut output = attached.output;

                    while let Some(result) = output.next().await {
                        match result {
                            Ok(log_output) => {
                                let message_bytes = match log_output {
                                    LogOutput::StdOut { message } |
                                    LogOutput::StdErr { message } |
                                    LogOutput::Console { message } |
                                    LogOutput::StdIn { message } => message,
                                };

                                let message = String::from_utf8_lossy(&message_bytes);
                                for line in message.lines() {
                                    let line = line.trim_end();
                                    if !line.is_empty() {
                                        if let Some(ref last) = last_line {
                                            if last == line {
                                                duplicate_count += 1;
                                                event_hub.broadcast_console_duplicate(&container_id, duplicate_count).await;
                                                continue;
                                            }
                                        }

                                        last_line = Some(line.to_string());
                                        duplicate_count = 1;
                                        event_hub.broadcast_console(&container_id, line).await;
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("Attach stream error for {}: {}", container_id, e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to attach to container {}: {}", container_id, e);
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }

    fn spawn_stats_streamer(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Same resilience approach as logs: stats streams can end on restarts.
            let mut backoff = Duration::from_millis(250);
            let start_time = std::time::Instant::now();
            let mut started_at: Option<DateTime<FixedOffset>> = None;

            loop {
                if !is_container_running(&docker, &container_id).await {
                    started_at = None;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                if started_at.is_none() {
                    if let Ok(info) = docker.inspect_container(&container_id, None).await {
                        if let Some(state) = info.state {
                            if let Some(started) = state.started_at {
                                if let Ok(parsed) = DateTime::parse_from_rfc3339(&started) {
                                    started_at = Some(parsed);
                                }
                            }
                        }
                    }
                }

                let opts = StatsOptions { stream: true, one_shot: false };
                let mut stream = docker.stats(&container_id, Some(opts));

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(stats) => {
                            backoff = Duration::from_millis(250);
                            let cpu_stats = &stats.cpu_stats;
                            let precpu_stats = &stats.precpu_stats;

                            let cpu_delta = cpu_stats.cpu_usage.total_usage
                                .saturating_sub(precpu_stats.cpu_usage.total_usage);
                            let system_delta = cpu_stats.system_cpu_usage.unwrap_or(0)
                                .saturating_sub(precpu_stats.system_cpu_usage.unwrap_or(0));
                            let num_cpus = cpu_stats.online_cpus.unwrap_or(1) as f64;

                            let cpu_percent = if system_delta > 0 && num_cpus > 0.0 {
                                ((cpu_delta as f64 / system_delta as f64) * num_cpus * 100.0 * 100.0).round() / 100.0
                            } else {
                                0.0
                            };

                            let memory_usage = stats.memory_stats.usage.unwrap_or(0);
                            let memory_limit = stats.memory_stats.limit.unwrap_or(0);

                            let (rx_bytes, tx_bytes) = stats.networks.as_ref()
                                .map(|networks| {
                                    networks.values().fold((0u64, 0u64), |(rx, tx), net| {
                                        (rx + net.rx_bytes, tx + net.tx_bytes)
                                    })
                                })
                                .unwrap_or((0, 0));

                            let uptime = started_at
                                .map(|ts| {
                                    let now = Utc::now().timestamp();
                                    let diff = now.saturating_sub(ts.timestamp());
                                    diff as u64
                                })
                                .unwrap_or_else(|| start_time.elapsed().as_secs());

                            event_hub.broadcast_stats(&container_id, EventContainerStats {
                                memory_bytes: memory_usage,
                                memory_limit_bytes: memory_limit,
                                cpu_percent,
                                network_rx_bytes: rx_bytes,
                                network_tx_bytes: tx_bytes,
                                uptime,
                                disk_bytes: 0,
                            }).await;
                        }
                        Err(e) => {
                            debug!("Stats stream error for {}: {}", container_id, e);
                            break;
                        }
                    }
                }

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        })
    }

    async fn execute_command(
        docker: &bollard::Docker,
        container_id: &str,
        command: &str,
        event_hub: &ContainerEventHub,
        mode: WsMode,
    ) {
        match mode {
            WsMode::Attached => {
                // In attached mode, commands are sent directly to stdin
                // This is handled by the attach stream, so we just broadcast the command
                event_hub.broadcast_console(container_id, &format!("> {}", command)).await;
            }
            WsMode::Exec => {
                // In exec mode, run command via docker exec
                let exec_config = CreateExecOptions {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(vec!["sh", "-c", command]),
                    ..Default::default()
                };

                match docker.create_exec(container_id, exec_config).await {
                    Ok(exec) => {
                        match docker.start_exec(&exec.id, None).await {
                            Ok(StartExecResults::Attached { mut output, .. }) => {
                                while let Some(result) = output.next().await {
                                    match result {
                                        Ok(log_output) => {
                                            let message_bytes = match log_output {
                                                LogOutput::StdOut { message } |
                                                LogOutput::StdErr { message } |
                                                LogOutput::Console { message } |
                                                LogOutput::StdIn { message } => message,
                                            };

                                            let text = String::from_utf8_lossy(&message_bytes);
                                            for line in text.lines() {
                                                let line = line.trim_end();
                                                if !line.is_empty() {
                                                    event_hub.broadcast_console(container_id, line).await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Exec output error for {}: {}", container_id, e);
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(StartExecResults::Detached) => {
                                debug!("Exec started in detached mode for {}", container_id);
                            }
                            Err(e) => {
                                error!("Failed to start exec for {}: {}", container_id, e);
                                event_hub.broadcast_console(container_id, &format!("Error: {}", e)).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to create exec for {}: {}", container_id, e);
                        event_hub.broadcast_console(container_id, &format!("Error: {}", e)).await;
                    }
                }
            }
        }
    }
}
