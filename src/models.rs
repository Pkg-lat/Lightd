use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateContainerRequest {
    pub image: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub startup_command: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub ports: Option<HashMap<String, String>>, // "80": "auto" format
    pub volumes: Option<Vec<VolumeMount>>,
    pub command: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub restart_policy: Option<String>,
    pub custom_uuid: Option<String>,
    pub limits: Option<ResourceLimits>,
    /// Install script - used during creation but not persisted to state/tracker
    pub install_content: Option<String>,
    /// Runtime config for startup/stop behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeConfig>,
    /// Optional request ID from panel for long-running operations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ResourceLimits {
    pub cpu: Option<String>,    // e.g., "2.0", "0.5"
    pub memory: Option<String>, // e.g., "2g", "512m"
    pub disk: Option<String>,   // e.g., "10g", "5g"
    pub swap: Option<String>,   // e.g., "1g", "512m" - swap memory limit
    pub pids: Option<u64>,      // PID limit - max number of processes
    pub threads: Option<u64>,   // Thread limit - max number of threads
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContainerTracker {
    pub custom_uuid: String,
    pub container_id: String,
    pub name: String,
    pub image: String,
    pub description: Option<String>,
    pub startup_command: Option<Vec<String>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub limits: ResourceLimits,
    pub allocated_ports: Vec<super::docker::network::PortAllocation>,
    pub attached_volumes: Vec<VolumeMount>,
    pub ports: HashMap<String, String>,
    pub env: Option<HashMap<String, String>>,
    pub status: String,
    /// Runtime config for state detection (stop command, startup log detection)
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PortConfig {
    pub container_port: String,
    pub host_port: Option<String>, // "auto" for auto-allocation or specific port
    pub host_ip: Option<String>,   // IP to bind to (default: "0.0.0.0")
    pub protocol: Option<String>,  // "tcp" or "udp" (default: "tcp")
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VolumeMount {
    pub source: String,
    pub target: String,
    pub read_only: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub image: String,
    pub state: String,
    pub status: String,
    pub created: String,
    pub ports: Vec<PortMapping>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PortMapping {
    pub private_port: u16,
    pub public_port: Option<u16>,
    pub host_ip: Option<String>,
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateVolumeRequest {
    pub name: String,
    pub driver: Option<String>,
    pub driver_opts: Option<HashMap<String, String>>,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub name: String,
    pub driver: String,
    pub mountpoint: String,
    pub created_at: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SuspendRequest {
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsuspendRequest {
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecRequest {
    pub command: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogsRequest {
    pub follow: Option<bool>,
    pub tail: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AttachResponse {
    pub exec_id: String,
    pub container_id: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateContainerRequest {
    pub update_content: String, // Shell script to run for updates
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,  // Docker image to use (overrides tracker)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_command: Option<Vec<String>>, // Startup command (overrides tracker)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateLimitsRequest {
    pub limits: ResourceLimits,
    pub restart_container: Option<bool>, // Whether to restart container to apply limits (default: false)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InstallationStatus {
    pub status: String, // "installing", "updating", "ready", "failed"
    pub progress: Option<String>,
    pub logs: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>, // Current container ID (may change during reinstall)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileSystemRequest {
    pub path: Option<String>, // Path to list, defaults to "/"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteFileRequest {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDirectoryRequest {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChmodRequest {
    pub path: String,
    pub permissions: String, // e.g., "755", "u+x", "go-w"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChownRequest {
    pub path: String,
    pub owner: String,
    pub group: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateArchiveRequest {
    #[serde(alias = "source_path")]
    pub source_paths: Vec<String>, // Support multiple files
    pub archive_path: String,
    pub compression: Option<String>, // "gzip", "bzip2", "xz", or None
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractArchiveRequest {
    pub archive_path: String,
    pub destination_path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateZipRequest {
    #[serde(alias = "source_path")]
    pub source_paths: Vec<String>, // Support multiple files
    pub zip_path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractZipRequest {
    pub zip_path: String,
    pub destination_path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopyFileRequest {
    pub source_path: String,
    pub destination_path: String,
    pub move_file: Option<bool>, // true for move, false for copy (default)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContainerLookupResponse {
    pub uuid: String,
    pub container_id: String,
    pub name: String,
    pub state: String,
    pub image: String,
    pub locked: Option<bool>,
    pub lock_reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebSocketTokenRequest {
    pub container_id: String, // Can be UUID or Docker container ID
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebSocketTokenResponse {
    pub token: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub container_id: String,
    pub container_uuid: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebSocketMessage {
    pub event: String,
    pub args: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContainerStats {
    pub memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub cpu_absolute: f64,
    pub network: NetworkStats,
    pub uptime: u64,
    pub state: String,
    pub disk_bytes: u64,
    #[serde(default)]
    pub is_suspended: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkStats {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}


/// Request to update container environment variables (triggers recreation)
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateEnvRequest {
    pub env: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Request to update container configuration (triggers recreation)
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateConfigRequest {
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<ResourceLimits>,
    pub ports: Option<HashMap<String, String>>,
    pub image: Option<String>,
    pub startup_command: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Runtime configuration for detecting server state
/// Matches the software JSON runtime field
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RuntimeConfig {
    /// Log string that indicates server has started (e.g., "Minecraft server started.")
    #[serde(rename = "start-up", default)]
    pub start_up: String,
    /// Command to send to stdin for graceful stop (e.g., "stop")
    #[serde(default)]
    pub stop: String,
}

/// Full reinstall request - panel sends everything needed
#[derive(Debug, Serialize, Deserialize)]
pub struct ReinstallRequest {
    /// Docker image to use
    pub image: String,
    /// Install script content
    pub install_script: String,
    /// Startup command (e.g., ["sh", "-c", "java -jar server.jar"])
    pub startup_command: Vec<String>,
    /// Environment variables
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    /// Port bindings (container_port -> host_port or "auto")
    #[serde(default)]
    pub ports: Option<HashMap<String, String>>,
    /// Resource limits
    #[serde(default)]
    pub limits: Option<ResourceLimits>,
    /// Runtime config for state detection
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Optional operation metadata for long-running actions
#[derive(Debug, Serialize, Deserialize)]
pub struct OperationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Request for stop action with optional runtime config
#[derive(Debug, Serialize, Deserialize)]
pub struct StopRequest {
    /// Runtime config for graceful stop (stop command to send to stdin)
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
}

/// Request for start action with optional runtime config
#[derive(Debug, Serialize, Deserialize)]
pub struct StartRequest {
    /// Runtime config for startup detection
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
}
