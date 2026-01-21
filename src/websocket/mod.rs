//! WebSocket module for lightd
//!
//! Channel-based WebSocket system for streaming container logs.
//! Based on docker-logs-streamer-via-web-socket approach:
//! - Use docker.logs() with follow: true and since timestamp
//! - Stream logs directly to websocket client
//! - Keep connection alive with pings
//! - Only stream logs when container is running

use serde::{Deserialize, Serialize};

pub mod connection;
pub mod token_manager;
pub mod handler;

pub use connection::*;
pub use token_manager::*;
pub use handler::WebSocketHandler;

/// WebSocket events (Server -> Client)
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WsEvent {
    /// Initial connection established
    Init,
    /// Console output from container
    ConsoleOutput,
    /// Duplicate console output detected (not sent as console_output)
    DuplicateEvent,
    /// Container status change
    Status,
    /// Container stats (CPU, memory, network, etc.)
    Stats,
    /// Error message
    Error,
    /// Daemon/system message
    DaemonMessage,
    /// Docker event (start, stop, die, etc.)
    DockerEvent,
    /// Command sent acknowledgment
    CommandSent,
}

/// WebSocket connection mode
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WsMode {
    /// Attached to container's main process (PID 1) - direct stdin/stdout
    Attached,
    /// Exec mode - runs commands via docker exec in a shell
    Exec,
}

impl Default for WsMode {
    fn default() -> Self {
        Self::Attached
    }
}

/// Client -> Server message types
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "event", content = "args")]
pub enum WsClientMessage {
    /// Send command to container stdin
    #[serde(rename = "send_command")]
    SendCommand(Vec<String>),
    /// Power action (start, stop, restart, kill)
    #[serde(rename = "power")]
    Power(Vec<String>),
    /// Switch WebSocket mode (attached/exec)
    #[serde(rename = "set_mode")]
    SetMode(Vec<String>),
    /// Request log tail (args: [tail])
    #[serde(rename = "request_logs")]
    RequestLogs(Vec<String>),
    /// Ping/keepalive
    #[serde(rename = "ping")]
    Ping,
}

/// WebSocket message format
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WsMessage {
    pub event: WsEvent,
    pub args: Vec<String>,
}

impl WsMessage {
    /// Create init message with container info
    pub fn init(container_id: &str, container_uuid: &str, status: &str) -> Self {
        Self {
            event: WsEvent::Init,
            args: vec![
                container_id.to_string(),
                container_uuid.to_string(),
                status.to_string(),
            ],
        }
    }

    /// Create console output message
    pub fn console_output(line: &str) -> Self {
        Self {
            event: WsEvent::ConsoleOutput,
            args: vec!["[container@pkg.lat]: ".to_string() + line],
        }
    }

    /// Create console duplicate message (just the count)
    pub fn console_duplicate(count: u32) -> Self {
        Self {
            event: WsEvent::ConsoleOutput,
            args: vec![format!("x{}", count)],
        }
    }

    /// Create status message
    pub fn status(state: &str) -> Self {
        Self {
            event: WsEvent::Status,
            args: vec![state.to_string()],
        }
    }

    /// Create error message
    /// not in use.
    pub fn error(msg: &str) -> Self {
        Self {
            event: WsEvent::Error,
            args: vec![msg.to_string()],
        }
    }

    /// Create daemon message
    pub fn daemon_message(msg: &str) -> Self {
        Self {
            event: WsEvent::DaemonMessage,
            args: vec!["[Lightd] ".to_string() + msg],
        }
    }
    
    /// Create mode changed message
    pub fn mode_changed(mode: &str) -> Self {
        Self {
            event: WsEvent::DaemonMessage,
            args: vec![format!("[Lightd] WebSocket mode changed to: {}", mode)],
        }
    }

    /// Create duplicate event message (when duplicate console output is detected)
    pub fn duplicate_event(line: &str) -> Self {
        Self {
            event: WsEvent::DuplicateEvent,
            args: vec![line.to_string()],
        }
    }

    /// Create docker event message (container lifecycle events from Docker)
    pub fn docker_event(action: &str, status: &str) -> Self {
        Self {
            event: WsEvent::DockerEvent,
            args: vec![action.to_string(), status.to_string()],
        }
    }

    /// Create command sent acknowledgment
    pub fn command_sent(command: &str) -> Self {
        Self {
            event: WsEvent::CommandSent,
            args: vec![command.to_string()],
        }
    }

    /// Create stats message with container resource usage (JSON string)
    /// not in use
    pub fn stats_json(stats_json: &str) -> Self {
        Self {
            event: WsEvent::Stats,
            args: vec![stats_json.to_string()],
        }
    }
    
    /// Create stats message with individual values
    pub fn stats(
        memory_bytes: u64,
        memory_limit_bytes: u64,
        cpu_percent: f64,
        network_rx_bytes: u64,
        network_tx_bytes: u64,
        uptime: u64,
        state: &str,
        disk_bytes: u64,
    ) -> Self {
        let stats_json = serde_json::json!({
            "memory_bytes": memory_bytes,
            "memory_limit_bytes": memory_limit_bytes,
            "cpu_absolute": cpu_percent,
            "network": {
                "rx_bytes": network_rx_bytes,
                "tx_bytes": network_tx_bytes,
            },
            "uptime": uptime,
            "state": state,
            "disk_bytes": disk_bytes,
        });
        Self {
            event: WsEvent::Stats,
            args: vec![stats_json.to_string()],
        }
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}