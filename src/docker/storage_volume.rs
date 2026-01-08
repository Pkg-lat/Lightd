//! Storage Volume Manager - Host-based volume management
//!
//! Manages volumes stored on the host filesystem (not Docker volumes).
//! Volumes are directories at {storage_path}/volumes/{volume_id}
//! These can be attached/detached from containers via mount swapping.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use tokio::process::Command;
use tracing::{info, warn, debug, error};
use uuid::Uuid;

/// Metadata for a storage volume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageVolumeMetadata {
    pub volume_id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub file_count: u64,
    /// Container UUID currently attached to (if any)
    pub attached_to: Option<String>,
    /// Labels for organization
    pub labels: std::collections::HashMap<String, String>,
}

/// Info returned when listing volumes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageVolumeInfo {
    pub volume_id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub file_count: u64,
    pub attached_to: Option<String>,
    pub path: String,
}

/// Storage volume manager
pub struct StorageVolumeManager {
    volumes_path: String,
}

impl StorageVolumeManager {
    pub fn new(storage_path: &str) -> Self {
        let volumes_path = format!("{}/volumes", storage_path);
        Self { volumes_path }
    }

    /// Initialize the volumes directory
    pub async fn init(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.volumes_path).await?;
        info!("Initialized storage volume manager at: {}", self.volumes_path);
        Ok(())
    }

    /// Create a new empty volume
    pub async fn create_volume(
        &self,
        name: &str,
        description: Option<String>,
        labels: Option<std::collections::HashMap<String, String>>,
    ) -> anyhow::Result<StorageVolumeInfo> {
        let volume_id = Uuid::new_v4().to_string();
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        
        info!("Creating storage volume {} at {}", volume_id, volume_path);
        
        // Create volume directory
        fs::create_dir_all(&volume_path).await?;
        
        // Create metadata
        let metadata = StorageVolumeMetadata {
            volume_id: volume_id.clone(),
            name: name.to_string(),
            description: description.clone(),
            created_at: Utc::now(),
            size_bytes: 0,
            file_count: 0,
            attached_to: None,
            labels: labels.unwrap_or_default(),
        };
        
        // Save metadata
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json).await?;
        
        info!("Storage volume {} created successfully", volume_id);
        
        Ok(StorageVolumeInfo {
            volume_id,
            name: name.to_string(),
            description,
            created_at: metadata.created_at,
            size_bytes: 0,
            file_count: 0,
            attached_to: None,
            path: volume_path,
        })
    }


    /// Create a volume from an existing container's data (clone)
    pub async fn create_from_container(
        &self,
        container_uuid: &str,
        name: &str,
        description: Option<String>,
    ) -> anyhow::Result<StorageVolumeInfo> {
        let source_path = format!("{}/{}", self.volumes_path, container_uuid);
        
        if !Path::new(&source_path).exists() {
            return Err(anyhow::anyhow!("Source container volume not found: {}", container_uuid));
        }
        
        let volume_id = Uuid::new_v4().to_string();
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        
        info!("Creating storage volume {} from container {}", volume_id, container_uuid);
        
        // Copy files from source to new volume
        let output = Command::new("cp")
            .args(&["-a", &source_path, &volume_path])
            .output()
            .await?;
        
        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to copy volume: {}", 
                String::from_utf8_lossy(&output.stderr)));
        }
        
        // Calculate size and file count
        let (size_bytes, file_count) = self.calculate_volume_stats(&volume_path).await?;
        
        // Create metadata
        let metadata = StorageVolumeMetadata {
            volume_id: volume_id.clone(),
            name: name.to_string(),
            description: description.clone(),
            created_at: Utc::now(),
            size_bytes,
            file_count,
            attached_to: None,
            labels: {
                let mut labels = std::collections::HashMap::new();
                labels.insert("cloned_from".to_string(), container_uuid.to_string());
                labels
            },
        };
        
        // Save metadata
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json).await?;
        
        info!("Storage volume {} created from container {} ({} bytes, {} files)", 
              volume_id, container_uuid, size_bytes, file_count);
        
        Ok(StorageVolumeInfo {
            volume_id,
            name: name.to_string(),
            description,
            created_at: metadata.created_at,
            size_bytes,
            file_count,
            attached_to: None,
            path: volume_path,
        })
    }

    /// List all storage volumes
    pub async fn list_volumes(&self) -> anyhow::Result<Vec<StorageVolumeInfo>> {
        let mut volumes = Vec::new();
        let mut entries = fs::read_dir(&self.volumes_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let volume_id = entry.file_name().to_string_lossy().to_string();
                let metadata_path = format!("{}/{}/volume_metadata.json", self.volumes_path, volume_id);
                
                // Only include directories with volume metadata (not container volumes)
                if let Ok(metadata_content) = fs::read_to_string(&metadata_path).await {
                    if let Ok(metadata) = serde_json::from_str::<StorageVolumeMetadata>(&metadata_content) {
                        volumes.push(StorageVolumeInfo {
                            volume_id: metadata.volume_id,
                            name: metadata.name,
                            description: metadata.description,
                            created_at: metadata.created_at,
                            size_bytes: metadata.size_bytes,
                            file_count: metadata.file_count,
                            attached_to: metadata.attached_to,
                            path: format!("{}/{}", self.volumes_path, volume_id),
                        });
                    }
                }
            }
        }
        
        volumes.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(volumes)
    }

    /// Get volume info by ID
    pub async fn get_volume(&self, volume_id: &str) -> anyhow::Result<StorageVolumeInfo> {
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        
        if !Path::new(&metadata_path).exists() {
            return Err(anyhow::anyhow!("Volume {} not found", volume_id));
        }
        
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let metadata: StorageVolumeMetadata = serde_json::from_str(&metadata_content)?;
        
        Ok(StorageVolumeInfo {
            volume_id: metadata.volume_id,
            name: metadata.name,
            description: metadata.description,
            created_at: metadata.created_at,
            size_bytes: metadata.size_bytes,
            file_count: metadata.file_count,
            attached_to: metadata.attached_to,
            path: volume_path,
        })
    }

    /// Delete a volume (must not be attached)
    pub async fn delete_volume(&self, volume_id: &str) -> anyhow::Result<()> {
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        
        if !Path::new(&metadata_path).exists() {
            return Err(anyhow::anyhow!("Volume {} not found", volume_id));
        }
        
        // Check if attached
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let metadata: StorageVolumeMetadata = serde_json::from_str(&metadata_content)?;
        
        if metadata.attached_to.is_some() {
            return Err(anyhow::anyhow!("Volume {} is attached to container {}", 
                volume_id, metadata.attached_to.unwrap()));
        }
        
        info!("Deleting storage volume {}", volume_id);
        fs::remove_dir_all(&volume_path).await?;
        info!("Storage volume {} deleted successfully", volume_id);
        
        Ok(())
    }


    /// Update volume attachment status
    pub async fn set_attached(&self, volume_id: &str, container_uuid: Option<&str>) -> anyhow::Result<()> {
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        
        if !Path::new(&metadata_path).exists() {
            return Err(anyhow::anyhow!("Volume {} not found", volume_id));
        }
        
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let mut metadata: StorageVolumeMetadata = serde_json::from_str(&metadata_content)?;
        
        metadata.attached_to = container_uuid.map(|s| s.to_string());
        
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json).await?;
        
        if let Some(uuid) = container_uuid {
            info!("Volume {} attached to container {}", volume_id, uuid);
        } else {
            info!("Volume {} detached", volume_id);
        }
        
        Ok(())
    }

    /// Update volume stats (size and file count)
    pub async fn update_stats(&self, volume_id: &str) -> anyhow::Result<StorageVolumeInfo> {
        let volume_path = format!("{}/{}", self.volumes_path, volume_id);
        let metadata_path = format!("{}/volume_metadata.json", volume_path);
        
        if !Path::new(&metadata_path).exists() {
            return Err(anyhow::anyhow!("Volume {} not found", volume_id));
        }
        
        let (size_bytes, file_count) = self.calculate_volume_stats(&volume_path).await?;
        
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let mut metadata: StorageVolumeMetadata = serde_json::from_str(&metadata_content)?;
        
        metadata.size_bytes = size_bytes;
        metadata.file_count = file_count;
        
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json).await?;
        
        Ok(StorageVolumeInfo {
            volume_id: metadata.volume_id,
            name: metadata.name,
            description: metadata.description,
            created_at: metadata.created_at,
            size_bytes,
            file_count,
            attached_to: metadata.attached_to,
            path: volume_path,
        })
    }

    /// Get the path for a volume
    pub fn get_volume_path(&self, volume_id: &str) -> String {
        format!("{}/{}", self.volumes_path, volume_id)
    }

    /// Check if a volume exists
    pub async fn volume_exists(&self, volume_id: &str) -> bool {
        let metadata_path = format!("{}/{}/volume_metadata.json", self.volumes_path, volume_id);
        Path::new(&metadata_path).exists()
    }

    /// Calculate volume size and file count
    async fn calculate_volume_stats(&self, volume_path: &str) -> anyhow::Result<(u64, u64)> {
        // Get file count
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
        
        // Get total size
        let output = Command::new("du")
            .args(&["-sb", volume_path])
            .output()
            .await?;
        
        let size_bytes = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        
        Ok((size_bytes, file_count))
    }
}
