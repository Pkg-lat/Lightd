//! WebSocket handler using broadcast channels
//!
//! Supports multiple connections per container.
//! Never blocks - all updates come via event hub.
//! Uses lock-free state manager for status queries.

use axum::extract::ws::{Message, WebSocket};
use bollard::container::{LogOutput, LogsOptions, StatsOptions, AttachContainerOptions};
use bollard::exec::{CreateExecOptions, StartExecResults};
use chrono::{DateTime, FixedOffset};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn, error};

use crate::types::AppState;
use crate::services::{ContainerEvent, ContainerEventHub, EventContainerStats};
use super::{WebSocketToken, WsMessage, WsMode};

#[derive(Debug, Deserialize)]
struct ClientMessage {
    event: String,
    args: Vec<String>,
}


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
        let current_status = Self::get_container_status(&docker, &container_id).await;
        let is_running = current_status == "running";

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

        // Start streaming if running
        let (log_task, attached_input_tx) = if is_running {
            let (task, input_tx) = Self::spawn_log_streamer(docker.clone(), container_id.clone(), event_hub.clone(), self.mode);
            (Some(task), input_tx)
        } else {
            (None, None)
        };

        let stats_task = if is_running {
            Some(Self::spawn_stats_streamer(docker.clone(), container_id.clone(), event_hub.clone()))
        } else {
            None
        };

        let mut container_running = is_running;
        let mut current_log_task: Option<tokio::task::JoinHandle<()>> = log_task;
        let mut attached_input_tx: Option<mpsc::UnboundedSender<String>> = attached_input_tx;
        let mut current_stats_task: Option<tokio::task::JoinHandle<()>> = stats_task;

        // Send initial log tail on connect (always try, even if stopped)
        if let Err(e) = Self::send_initial_logs(&docker, &container_id, &mut ws_tx).await {
            debug!("Failed to send initial logs for {}: {}", container_id, e);
        }

        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    match event {
                        Ok(ContainerEvent::StateChanged { state }) => {
                            let was_running = container_running;
                            container_running = state == "running";

                            let msg = WsMessage::status(&state).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() {
                                break;
                            }

                            if container_running && !was_running {
                                let (task, input_tx) = Self::spawn_log_streamer(
                                    docker.clone(), container_id.clone(), event_hub.clone(), self.mode,
                                );
                                current_log_task = Some(task);
                                attached_input_tx = input_tx;
                                current_stats_task = Some(Self::spawn_stats_streamer(
                                    docker.clone(), container_id.clone(), event_hub.clone(),
                                ));
                            } else if !container_running && was_running {
                                if let Some(task) = current_log_task.take() { task.abort(); }
                                if let Some(task) = current_stats_task.take() { task.abort(); }
                                attached_input_tx = None;
                            }
                        }
                        Ok(ContainerEvent::ConsoleOutput { line }) => {
                            let msg = WsMessage::console_output(&line).to_json();
                            if ws_tx.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Ok(ContainerEvent::Stats(stats)) => {
                            let msg = WsMessage::stats(
                                stats.memory_bytes, stats.memory_limit_bytes, stats.cpu_percent,
                                stats.network_rx_bytes, stats.network_tx_bytes, stats.uptime,
                                if container_running { "running" } else { "stopped" },
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
                                            if container_running {
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

                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if ws_tx.send(Message::Ping(vec![])).await.is_err() { break; }
                }
            }
        }

        if let Some(task) = current_log_task { task.abort(); }
        if let Some(task) = current_stats_task { task.abort(); }
        event_hub.unsubscribe(&container_id).await;
        info!("WebSocket closed for container: {}", container_id);
    }

    /// Get container status directly from Docker (no state manager lock)
    async fn get_container_status(docker: &bollard::Docker, container_id: &str) -> String {
        match tokio::time::timeout(
            Duration::from_secs(2),
            docker.inspect_container(container_id, None)
        ).await {
            Ok(Ok(info)) => {
                if let Some(state) = info.state {
                    if state.running == Some(true) { "running".to_string() }
                    else if state.paused == Some(true) { "paused".to_string() }
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
    
    /// Stream logs in attached mode - uses docker.logs() API
    async fn stream_logs_attached(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
        mut input_rx: mpsc::UnboundedReceiver<String>,
    ) {
            let mut backoff = Duration::from_millis(250);

            loop {
                if !is_container_running(&docker, &container_id).await {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                let attach_opts = AttachContainerOptions::<String> {
                    stdin: Some(true),
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    logs: Some(true),
                    ..Default::default()
                };

                match docker.attach_container(&container_id, Some(attach_opts)).await {
                    Ok(attached) => {
                        info!("Attached to container {} in attached mode", container_id);
                        backoff = Duration::from_millis(250);

                        let mut output = attached.output;
                        let mut input = attached.input;

                        loop {
                            tokio::select! {
                                cmd = input_rx.recv() => {
                                    match cmd {
                                        Some(command) => {
                                            let payload = format!("{}\n", command);
                                            if let Err(e) = input.write_all(payload.as_bytes()).await {
                                                debug!("Failed to write to attached stdin for {}: {}", container_id, e);
                                                break;
                                            }
                                            let _ = input.flush().await;
                                        }
                                        None => {
                                            return;
                                        }
                                    }
                                }
                                result = output.next() => {
                                    match result {
                                        Some(Ok(log_output)) => {
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
                                                    event_hub.broadcast_console(&container_id, line).await;
                                                }
                                            }
                                        }
                                        Some(Err(e)) => {
                                            debug!("Attach stream error for {}: {}", container_id, e);
                                            break;
                                        }
                                        None => {
                                            break;
                                        }
                                    }
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

    async fn send_initial_logs(
        docker: &bollard::Docker,
        container_id: &str,
        ws_tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    ) -> Result<(), String> {
        let opts = LogsOptions::<String> {
            follow: false,
            stdout: true,
            stderr: true,
            since: 0,
            timestamps: true,
            tail: "200".to_string(),
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
    
    /// Stream logs in exec mode - attaches to container's main process (PID 1)
    async fn stream_logs_exec(
        docker: Arc<bollard::Docker>,
        container_id: String,
        event_hub: Arc<ContainerEventHub>,
    ) {
        info!("Starting exec mode log streamer for {}", container_id);
        
        // In exec mode, we attach to the container's main process
        // This gives us direct stdin/stdout access
        let mut backoff = Duration::from_millis(250);
        
        loop {
            if !is_container_running(&docker, &container_id).await {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            
            // Attach to container with stdin/stdout/stderr
            let attach_opts = AttachContainerOptions::<String> {
                stdin: Some(true),
                stdout: Some(true),
                stderr: Some(true),
                stream: Some(true),
                logs: Some(true),  // Get recent logs too
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
            
            // Reconnect after backoff
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

            loop {
                if !is_container_running(&docker, &container_id).await {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
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

                            let uptime = start_time.elapsed().as_secs();

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
