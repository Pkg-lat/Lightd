use bollard::Docker;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use tokio::process::Command;
use tracing::{info, warn, debug};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::state_manager::{StateManager, ContainerState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub snapshot_id: String,
    pub container_uuid: String,
    pub container_id: String,
    pub container_name: String,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub file_count: u64,
    pub container_config: ContainerState,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub container_uuid: String,
    pub container_name: String,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub file_count: u64,
    pub status: String,
}

pub struct SnapshotManager {
    docker: Docker,
    snapshots_path: String,
}

impl SnapshotManager {
    pub fn new(docker: Docker, storage_path: &str) -> Self {
        let snapshots_path = format!("{}/snapshots", storage_path);
        Self {
            docker,
            snapshots_path,
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.snapshots_path).await?;
        info!("Initialized snapshot storage at: {}", self.snapshots_path);
        Ok(())
    }

    /// Create a simple file-based snapshot of container workspace
    pub async fn create_snapshot(
        &self,
        container_id: &str,
        container_uuid: &str,
        state_manager: &StateManager,
    ) -> anyhow::Result<String> {
        let snapshot_id = Uuid::new_v4().to_string();
        let snapshot_dir = format!("{}/{}", self.snapshots_path, snapshot_id);
        
        info!("Creating workspace snapshot {} for container {}", snapshot_id, container_uuid);
        
        // Create snapshot directory
        fs::create_dir_all(&snapshot_dir).await?;
        
        // Get container state
        let container_state = state_manager.get_container(container_uuid)
            .ok_or_else(|| anyhow::anyhow!("Container not found in state manager"))?;
        
        // Get the volume path from storage base path
        let volume_path = format!("{}/volumes/{}", 
            self.snapshots_path.trim_end_matches("/snapshots"), 
            container_uuid
        );
        
        // 1. Create tar archive of workspace files directly from host mount
        info!("Creating tar archive of workspace files from {}", volume_path);
        let archive_path = format!("{}/workspace.tar.gz", snapshot_dir);
        let file_count = self.create_workspace_archive_direct(&volume_path, &archive_path).await?;
        
        // 2. Calculate archive size
        let size_bytes = fs::metadata(&archive_path).await?.len();
        
        // 3. Create metadata
        let metadata = SnapshotMetadata {
            snapshot_id: snapshot_id.clone(),
            container_uuid: container_uuid.to_string(),
            container_id: container_id.to_string(),
            container_name: container_state.name.clone(),
            created_at: Utc::now(),
            size_bytes,
            file_count,
            container_config: container_state.clone(),
        };
        
        // 4. Save metadata
        let metadata_path = format!("{}/metadata.json", snapshot_dir);
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json).await?;
        
        info!("Snapshot {} created successfully ({} bytes, {} files)", 
              snapshot_id, size_bytes, file_count);
        Ok(snapshot_id)
    }

    /// Restore workspace files from snapshot
    pub async fn restore_snapshot(
        &self,
        snapshot_id: &str,
        container_uuid: &str,
    ) -> anyhow::Result<()> {
        let snapshot_dir = format!("{}/{}", self.snapshots_path, snapshot_id);
        let metadata_path = format!("{}/metadata.json", snapshot_dir);
        let archive_path = format!("{}/workspace.tar.gz", snapshot_dir);
        
        info!("Restoring workspace snapshot {} to container {}", snapshot_id, container_uuid);
        
        // Load metadata
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let metadata: SnapshotMetadata = serde_json::from_str(&metadata_content)?;
        
        // Check if archive exists
        if !Path::new(&archive_path).exists() {
            return Err(anyhow::anyhow!("Snapshot archive not found: {}", archive_path));
        }
        
        // Get the volume path from storage base path
        let volume_path = format!("{}/volumes/{}", 
            self.snapshots_path.trim_end_matches("/snapshots"), 
            container_uuid
        );
        
        // 1. Clear workspace directory directly on host
        info!("Clearing workspace directory at {}", volume_path);
        self.clear_workspace_direct(&volume_path).await?;
        
        // 2. Extract archive to workspace directly on host
        info!("Extracting archive to workspace...");
        self.extract_workspace_archive_direct(&volume_path, &archive_path).await?;
        
        info!("Snapshot {} restored successfully ({} files)", 
              snapshot_id, metadata.file_count);
        Ok(())
    }

    /// List all available snapshots
    pub async fn list_snapshots(&self) -> anyhow::Result<Vec<SnapshotInfo>> {
        let mut snapshots = Vec::new();
        let mut entries = fs::read_dir(&self.snapshots_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let snapshot_id = entry.file_name().to_string_lossy().to_string();
                let metadata_path = format!("{}/{}/metadata.json", self.snapshots_path, snapshot_id);
                
                if let Ok(metadata_content) = fs::read_to_string(&metadata_path).await {
                    if let Ok(metadata) = serde_json::from_str::<SnapshotMetadata>(&metadata_content) {
                        snapshots.push(SnapshotInfo {
                            snapshot_id: metadata.snapshot_id,
                            container_uuid: metadata.container_uuid,
                            container_name: metadata.container_name,
                            created_at: metadata.created_at,
                            size_bytes: metadata.size_bytes,
                            file_count: metadata.file_count,
                            status: "available".to_string(),
                        });
                    }
                }
            }
        }
        
        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(snapshots)
    }

    /// Delete a snapshot
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> anyhow::Result<()> {
        let snapshot_dir = format!("{}/{}", self.snapshots_path, snapshot_id);
        
        if !Path::new(&snapshot_dir).exists() {
            return Err(anyhow::anyhow!("Snapshot {} not found", snapshot_id));
        }
        
        info!("Deleting snapshot {}", snapshot_id);
        fs::remove_dir_all(&snapshot_dir).await?;
        info!("Snapshot {} deleted successfully", snapshot_id);
        Ok(())
    }

    /// Get snapshot metadata
    pub async fn get_snapshot_metadata(&self, snapshot_id: &str) -> anyhow::Result<SnapshotMetadata> {
        let metadata_path = format!("{}/{}/metadata.json", self.snapshots_path, snapshot_id);
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let metadata: SnapshotMetadata = serde_json::from_str(&metadata_content)?;
        Ok(metadata)
    }

    // Private helper methods - Direct filesystem operations

    async fn create_workspace_archive_direct(&self, volume_path: &str, archive_path: &str) -> anyhow::Result<u64> {
        // Check if volume path exists
        if !Path::new(volume_path).exists() {
            return Err(anyhow::anyhow!("Volume path does not exist: {}", volume_path));
        }

        // Count files
        let output = Command::new("sh")
            .args(&[
                "-c",
                &format!("cd '{}' && find . -type f 2>/dev/null | wc -l", volume_path)
            ])
            .output()
            .await?;

        let file_count = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(0);

        // Create tar archive directly from host filesystem
        let output = Command::new("tar")
            .args(&[
                "-czf",
                archive_path,
                "-C",
                volume_path,
                "."
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.is_empty() {
                warn!("Archive creation had warnings: {}", stderr);
            }
        }

        debug!("Created workspace archive with {} files from {}", file_count, volume_path);
        Ok(file_count)
    }

    async fn clear_workspace_direct(&self, volume_path: &str) -> anyhow::Result<()> {
        // Check if volume path exists
        if !Path::new(volume_path).exists() {
            info!("Volume path does not exist, creating: {}", volume_path);
            fs::create_dir_all(volume_path).await?;
            return Ok(());
        }

        // Clear all files in the volume directory (but keep the directory itself)
        let output = Command::new("sh")
            .args(&[
                "-c",
                &format!("cd '{}' && rm -rf * .[!.]* ..?* 2>/dev/null || true", volume_path)
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.is_empty() {
                warn!("Workspace clear had warnings: {}", stderr);
            }
        }

        debug!("Cleared workspace directory at {}", volume_path);
        Ok(())
    }

    async fn extract_workspace_archive_direct(&self, volume_path: &str, archive_path: &str) -> anyhow::Result<()> {
        // Check if archive exists
        if !Path::new(archive_path).exists() {
            return Err(anyhow::anyhow!("Archive does not exist: {}", archive_path));
        }

        // Ensure volume directory exists
        if !Path::new(volume_path).exists() {
            fs::create_dir_all(volume_path).await?;
        }

        // Extract tar archive directly to host filesystem
        let output = Command::new("tar")
            .args(&[
                "-xzf",
                archive_path,
                "-C",
                volume_path
            ])
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to extract archive: {}", 
                String::from_utf8_lossy(&output.stderr)));
        }

        debug!("Extracted workspace archive to {}", volume_path);
        Ok(())
    }
}