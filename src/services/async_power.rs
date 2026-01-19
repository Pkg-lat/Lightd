//! Async Power Actions - Truly non-blocking container power operations
//!
//! All operations are fire-and-forget. Status updates are sent via the event hub.
//! No locks are held during Docker operations.
//!
//! Power flow:
//! - Start: Docker start → watch logs for runtime.start-up → "running" → callback to panel
//! - Stop: Send runtime.stop to stdin → wait for exit (no timeout) → "stopped" → callback to panel
//! - Kill: Docker stop + SIGTERM → "stopped" → callback to panel
//! - Restart: Stop → Start

use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};
use bollard::Docker;
use bollard::container::{KillContainerOptions, AttachContainerOptions, LogsOptions};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use regex::Regex;

use super::container_events::{ContainerEventHub, ContainerEvent};
use crate::remote::Remote;

/// Power action types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerAction {
    Start,
    Stop,
    Kill,
    Restart,
}

impl std::fmt::Display for PowerAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PowerAction::Start => write!(f, "start"),
            PowerAction::Stop => write!(f, "stop"),
            PowerAction::Kill => write!(f, "kill"),
            PowerAction::Restart => write!(f, "restart"),
        }
    }
}

/// Runtime config for a container (from software JSON)
#[derive(Debug, Clone, Default)]
pub struct ContainerRuntime {
    /// Log string that indicates server has started
    pub start_up: String,
    /// Command to send to stdin for graceful stop
    pub stop: String,
}

/// Tracks in-flight power actions to prevent duplicate concurrent operations
struct ActionTracker {
    /// container_id -> current action
    actions: HashMap<String, PowerAction>,
    /// container_id -> runtime config
    runtimes: HashMap<String, ContainerRuntime>,
}

impl ActionTracker {
    fn new() -> Self {
        Self { 
            actions: HashMap::new(),
            runtimes: HashMap::new(),
        }
    }

    fn try_start(&mut self, container_id: &str, action: PowerAction) -> bool {
        if let Some(current) = self.actions.get(container_id) {
            // Kill can override any other action (stop, start, restart)
            if action == PowerAction::Kill {
                info!("Kill action overriding current {:?} action for {}", current, container_id);
                self.actions.insert(container_id.to_string(), action);
                true
            } else {
                false
            }
        } else {
            self.actions.insert(container_id.to_string(), action);
            true
        }
    }

    fn finish(&mut self, container_id: &str) {
        self.actions.remove(container_id);
    }

    fn current_action(&self, container_id: &str) -> Option<PowerAction> {
        self.actions.get(container_id).copied()
    }

    fn set_runtime(&mut self, container_id: &str, runtime: ContainerRuntime) {
        self.runtimes.insert(container_id.to_string(), runtime);
    }

    fn get_runtime(&self, container_id: &str) -> Option<ContainerRuntime> {
        self.runtimes.get(container_id).cloned()
    }
}

/// Async power action manager
pub struct AsyncPowerManager {
    docker: Arc<Docker>,
    event_hub: Arc<ContainerEventHub>,
    tracker: Arc<RwLock<ActionTracker>>,
    remote: Option<Arc<Remote>>,
}

impl AsyncPowerManager {
    pub fn new(docker: Arc<Docker>, event_hub: Arc<ContainerEventHub>) -> Self {
        Self {
            docker,
            event_hub,
            tracker: Arc::new(RwLock::new(ActionTracker::new())),
            remote: None,
        }
    }

    /// Set remote client for panel callbacks
    pub fn with_remote(mut self, remote: Arc<Remote>) -> Self {
        self.remote = Some(remote);
        self
    }

    /// Set runtime config for a container
    pub async fn set_runtime(&self, container_id: &str, start_up: &str, stop: &str) {
        let mut tracker = self.tracker.write().await;
        tracker.set_runtime(container_id, ContainerRuntime {
            start_up: start_up.to_string(),
            stop: stop.to_string(),
        });
    }

    /// Check if container has an action in progress
    pub async fn has_pending_action(&self, container_id: &str) -> Option<String> {
        self.tracker.read().await.current_action(container_id).map(|a| a.to_string())
    }

    /// Send state callback to panel
    async fn callback_state(remote: &Option<Arc<Remote>>, uuid: &str, state: &str) {
        if let Some(ref r) = remote {
            r.send_container_state(uuid, state).await;
        }
    }

    /// Start a container - fire and forget
    /// Watches logs for runtime.start-up string to detect "running" state
    /// Callbacks to panel: starting → running
    pub async fn start(&self, container_id: String, uuid: String) -> Result<(), String> {
        {
            let mut tracker = self.tracker.write().await;
            if !tracker.try_start(&container_id, PowerAction::Start) {
                return Err(format!("Container {} already has an action in progress", container_id));
            }
        }

        let docker = self.docker.clone();
        let hub = self.event_hub.clone();
        let tracker = self.tracker.clone();
        let remote = self.remote.clone();
        let cid = container_id.clone();
        let uid = uuid.clone();

        let runtime = tracker.read().await.get_runtime(&cid);

        tokio::spawn(async move {
            hub.broadcast(&cid, ContainerEvent::PowerActionStarted {
                action: "start".to_string(),
            }).await;

            // Broadcast "starting" state
            hub.broadcast_state(&cid, "starting").await;
            Self::callback_state(&remote, &uid, "starting").await;

            let result = Self::do_start(&docker, &cid).await;
            
            if result.is_ok() {
                // If we have a startup detection string, wait for it
                if let Some(ref rt) = runtime {
                    if !rt.start_up.is_empty() {
                        info!("Watching logs for startup string: '{}'", rt.start_up);
                        let detected = Self::wait_for_startup_log(&docker, &cid, &rt.start_up, 300).await;
                        if detected {
                            info!("Startup detected for container {} - server is running", cid);
                        } else {
                            warn!("Startup detection timed out for {}, assuming running", cid);
                        }
                    }
                }
                // Container is now running
                hub.broadcast_state(&cid, "running").await;
                Self::callback_state(&remote, &uid, "running").await;
            } else {
                hub.broadcast_state(&cid, "stopped").await;
                Self::callback_state(&remote, &uid, "stopped").await;
            }

            hub.broadcast(&cid, ContainerEvent::PowerActionCompleted {
                action: "start".to_string(),
                success: result.is_ok(),
                message: result.as_ref().map(|_| "started".to_string())
                    .unwrap_or_else(|e| e.clone()),
            }).await;

            tracker.write().await.finish(&cid);

            match result {
                Ok(_) => info!("Container {} started successfully", cid),
                Err(e) => error!("Failed to start container {}: {}", cid, e),
            }
        });

        Ok(())
    }

    /// Stop a container - fire and forget
    /// Sends runtime.stop command to stdin and waits for container to exit
    /// NO force stop - only kill action does that
    /// Callbacks to panel: stopping → stopped
    pub async fn stop(&self, container_id: String, uuid: String) -> Result<(), String> {
        {
            let mut tracker = self.tracker.write().await;
            if !tracker.try_start(&container_id, PowerAction::Stop) {
                return Err(format!("Container {} already has an action in progress", container_id));
            }
        }

        let docker = self.docker.clone();
        let hub = self.event_hub.clone();
        let tracker = self.tracker.clone();
        let remote = self.remote.clone();
        let cid = container_id.clone();
        let uid = uuid.clone();

        let runtime = tracker.read().await.get_runtime(&cid);

        tokio::spawn(async move {
            hub.broadcast(&cid, ContainerEvent::PowerActionStarted {
                action: "stop".to_string(),
            }).await;

            // Broadcast "stopping" state
            hub.broadcast_state(&cid, "stopping").await;
            Self::callback_state(&remote, &uid, "stopping").await;

            let result = if let Some(ref rt) = runtime {
                if !rt.stop.is_empty() {
                    info!("Sending stop command '{}' to container {}", rt.stop, cid);
                    Self::do_graceful_stop_no_timeout(&docker, &cid, &rt.stop).await
                } else {
                    // No stop command defined - just wait for container to stop on its own
                    // This shouldn't happen in practice, but handle it gracefully
                    warn!("No stop command defined for {}, container may not stop", cid);
                    Self::wait_for_container_exit(&docker, &cid).await
                }
            } else {
                warn!("No runtime config for {}, container may not stop", cid);
                Self::wait_for_container_exit(&docker, &cid).await
            };
            
            // Container is now stopped
            hub.broadcast_state(&cid, "stopped").await;
            Self::callback_state(&remote, &uid, "stopped").await;

            hub.broadcast(&cid, ContainerEvent::PowerActionCompleted {
                action: "stop".to_string(),
                success: result.is_ok(),
                message: result.as_ref().map(|_| "stopped".to_string())
                    .unwrap_or_else(|e| e.clone()),
            }).await;

            tracker.write().await.finish(&cid);

            match result {
                Ok(_) => info!("Container {} stopped successfully", cid),
                Err(e) => warn!("Stop container {} returned error: {}", cid, e),
            }
        });

        Ok(())
    }

    /// Kill a container - fire and forget
    /// Uses Docker stop + SIGTERM - this is the ONLY way to force stop
    /// Callbacks to panel: stopping → stopped
    pub async fn kill(&self, container_id: String, uuid: String) -> Result<(), String> {
        {
            let mut tracker = self.tracker.write().await;
            if !tracker.try_start(&container_id, PowerAction::Kill) {
                return Err(format!("Container {} already has an action in progress", container_id));
            }
        }

        let docker = self.docker.clone();
        let hub = self.event_hub.clone();
        let tracker = self.tracker.clone();
        let remote = self.remote.clone();
        let cid = container_id.clone();
        let uid = uuid.clone();

        tokio::spawn(async move {
            hub.broadcast(&cid, ContainerEvent::PowerActionStarted {
                action: "kill".to_string(),
            }).await;

            // Broadcast "stopping" state (kill is still a stop, just forced)
            hub.broadcast_state(&cid, "stopping").await;
            Self::callback_state(&remote, &uid, "stopping").await;

            // Use Docker kill directly - instant SIGKILL like Pterodactyl
            let result = Self::do_docker_kill(&docker, &cid).await;
            
            // Container is now stopped
            hub.broadcast_state(&cid, "stopped").await;
            Self::callback_state(&remote, &uid, "stopped").await;

            hub.broadcast(&cid, ContainerEvent::PowerActionCompleted {
                action: "kill".to_string(),
                success: result.is_ok(),
                message: result.as_ref().map(|_| "killed".to_string())
                    .unwrap_or_else(|e| e.clone()),
            }).await;

            tracker.write().await.finish(&cid);

            match result {
                Ok(_) => info!("Container {} killed successfully", cid),
                Err(e) => warn!("Kill container {} returned error: {}", cid, e),
            }
        });

        Ok(())
    }

    /// Restart a container - fire and forget
    /// Callbacks to panel: stopping → stopped → starting → running
    pub async fn restart(&self, container_id: String, uuid: String) -> Result<(), String> {
        {
            let mut tracker = self.tracker.write().await;
            if !tracker.try_start(&container_id, PowerAction::Restart) {
                return Err(format!("Container {} already has an action in progress", container_id));
            }
        }

        let docker = self.docker.clone();
        let hub = self.event_hub.clone();
        let tracker_arc = self.tracker.clone();
        let remote = self.remote.clone();
        let cid = container_id.clone();
        let uid = uuid.clone();

        let runtime = tracker_arc.read().await.get_runtime(&cid);

        tokio::spawn(async move {
            hub.broadcast(&cid, ContainerEvent::PowerActionStarted {
                action: "restart".to_string(),
            }).await;

            // Stop phase
            hub.broadcast_state(&cid, "stopping").await;
            Self::callback_state(&remote, &uid, "stopping").await;
            hub.broadcast_message(&cid, "Stopping container...").await;
            
            let _ = if let Some(ref rt) = runtime {
                if !rt.stop.is_empty() {
                    Self::do_graceful_stop_no_timeout(&docker, &cid, &rt.stop).await
                } else {
                    Self::wait_for_container_exit(&docker, &cid).await
                }
            } else {
                Self::wait_for_container_exit(&docker, &cid).await
            };
            
            hub.broadcast_state(&cid, "stopped").await;
            Self::callback_state(&remote, &uid, "stopped").await;
            
            tokio::time::sleep(Duration::from_millis(500)).await;
            
            // Start phase
            hub.broadcast_state(&cid, "starting").await;
            Self::callback_state(&remote, &uid, "starting").await;
            hub.broadcast_message(&cid, "Starting container...").await;
            
            let result = Self::do_start(&docker, &cid).await;
            
            if result.is_ok() {
                if let Some(ref rt) = runtime {
                    if !rt.start_up.is_empty() {
                        let _ = Self::wait_for_startup_log(&docker, &cid, &rt.start_up, 300).await;
                    }
                }
                hub.broadcast_state(&cid, "running").await;
                Self::callback_state(&remote, &uid, "running").await;
            } else {
                hub.broadcast_state(&cid, "stopped").await;
                Self::callback_state(&remote, &uid, "stopped").await;
            }

            hub.broadcast(&cid, ContainerEvent::PowerActionCompleted {
                action: "restart".to_string(),
                success: result.is_ok(),
                message: result.as_ref().map(|_| "restarted".to_string())
                    .unwrap_or_else(|e| e.clone()),
            }).await;

            tracker_arc.write().await.finish(&cid);

            match result {
                Ok(_) => info!("Container {} restarted successfully", cid),
                Err(e) => error!("Failed to restart container {}: {}", cid, e),
            }
        });

        Ok(())
    }

    // ==================== Internal helper methods ====================

    async fn do_start(docker: &Docker, container_id: &str) -> Result<(), String> {
        match tokio::time::timeout(
            Duration::from_secs(30),
            docker.start_container::<String>(container_id, None)
        ).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                let err_str = e.to_string();
                if err_str.contains("already started") || err_str.contains("is already running") {
                    Ok(())
                } else {
                    Err(err_str)
                }
            }
            Err(_) => Err("Timeout starting container".to_string()),
        }
    }

    /// Graceful stop - send command to stdin and wait for container to exit
    /// NO TIMEOUT - waits indefinitely for container to stop
    /// Only kill action should force stop
    async fn do_graceful_stop_no_timeout(docker: &Docker, container_id: &str, stop_cmd: &str) -> Result<(), String> {
        // Send stop command to stdin
        let options = AttachContainerOptions::<String> {
            stdin: Some(true),
            stdout: Some(false),
            stderr: Some(false),
            stream: Some(true),
            ..Default::default()
        };

        match docker.attach_container(container_id, Some(options)).await {
            Ok(attach_result) => {
                let mut input = attach_result.input;
                let trimmed = stop_cmd.trim();

                let write_result = if trimmed.eq_ignore_ascii_case("^^c") || trimmed.eq_ignore_ascii_case("^c") {
                    // Send Ctrl+C (SIGINT) to attached stdin
                    input.write_all(&[0x03]).await
                } else {
                    let cmd_with_newline = format!("{}\n", stop_cmd);
                    input.write_all(cmd_with_newline.as_bytes()).await
                };

                if let Err(e) = write_result {
                    warn!("Failed to send stop command: {}", e);
                } else {
                    let _ = input.flush().await;
                    info!("Sent stop command '{}' to container {}", stop_cmd, container_id);
                }
            }
            Err(e) => {
                warn!("Failed to attach for stop command: {}", e);
                // Container might already be stopped
                return Self::wait_for_container_exit(docker, container_id).await;
            }
        }

        // Wait indefinitely for container to exit
        Self::wait_for_container_exit(docker, container_id).await
    }

    /// Wait for container to exit - no timeout
    /// Polls every second until container is not running
    async fn wait_for_container_exit(docker: &Docker, container_id: &str) -> Result<(), String> {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            match docker.inspect_container(container_id, None).await {
                Ok(info) => {
                    if let Some(state) = info.state {
                        if state.running != Some(true) {
                            info!("Container {} exited", container_id);
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    // Container might not exist anymore
                    let err_str = e.to_string();
                    if err_str.contains("No such container") {
                        return Ok(());
                    }
                    debug!("Error inspecting container {}: {}", container_id, e);
                }
            }
        }
    }

    /// Docker kill - SIGTERM (graceful signal)
    async fn do_docker_kill(docker: &Docker, container_id: &str) -> Result<(), String> {
        let opts = KillContainerOptions { signal: "SIGTERM" };
        
        match tokio::time::timeout(
            Duration::from_secs(5),
            docker.kill_container(container_id, Some(opts))
        ).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                let err_str = e.to_string();
                if err_str.contains("is not running") || err_str.contains("No such container") {
                    Ok(())
                } else {
                    Err(err_str)
                }
            }
            Err(_) => Err("Timeout killing container".to_string()),
        }
    }

    async fn wait_for_startup_log(docker: &Docker, container_id: &str, startup_str: &str, timeout_secs: u64) -> bool {
        let startup_regex = Regex::new(startup_str).ok();
        let opts = LogsOptions::<String> {
            follow: true,
            stdout: true,
            stderr: true,
            since: 0,
            timestamps: false,
            tail: "0".to_string(),
            ..Default::default()
        };

        let mut stream = docker.logs(container_id, Some(opts));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    debug!("Startup detection timed out for {}", container_id);
                    return false;
                }
                result = stream.next() => {
                    match result {
                        Some(Ok(log_output)) => {
                            let message = match log_output {
                                bollard::container::LogOutput::StdOut { message } |
                                bollard::container::LogOutput::StdErr { message } |
                                bollard::container::LogOutput::Console { message } |
                                bollard::container::LogOutput::StdIn { message } => {
                                    String::from_utf8_lossy(&message).to_string()
                                }
                            };
                            
                            let cleaned = strip_ansi(&message);

                            let matched = if let Some(ref re) = startup_regex {
                                re.is_match(&cleaned)
                            } else {
                                cleaned.contains(startup_str)
                            };

                            if matched {
                                debug!("Found startup pattern '{}' in logs", startup_str);
                                return true;
                            }
                        }
                        Some(Err(e)) => {
                            debug!("Log stream error: {}", e);
                            return false;
                        }
                        None => {
                            debug!("Log stream ended");
                            return false;
                        }
                    }
                }
            }
        }
    }
}

fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if let Some('[') = chars.peek().copied() {
                chars.next();
                while let Some(c) = chars.next() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
                continue;
            }
        }

        out.push(ch);
    }

    out
}
