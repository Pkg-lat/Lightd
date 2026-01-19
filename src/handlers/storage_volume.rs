//! Storage Volume Handlers - API endpoints for host-based volume management
//!
//! Provides endpoints for creating, listing, deleting volumes and swapping mounts.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    docker::{StorageVolumeManager, StorageVolumeInfo},
    models::ApiResponse,
    types::AppState,
};

#[derive(Debug, Deserialize)]
pub struct CreateStorageVolumeRequest {
    pub name: String,
    pub description: Option<String>,
    pub labels: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CloneVolumeRequest {
    pub source_container_uuid: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SwapMountRequest {
    pub new_volume_id: String,
}

#[derive(Debug, Serialize)]
pub struct MountInfo {
    pub container_uuid: String,
    pub current_volume_path: String,
    pub is_default_volume: bool,
}

#[derive(Debug, Serialize)]
pub struct SwapMountResponse {
    pub container_uuid: String,
    pub old_volume_path: String,
    pub new_volume_path: String,
    pub new_container_id: String,
    pub message: String,
}

/// Create a new empty storage volume
pub async fn create_storage_volume(
    State(state): State<AppState>,
    Json(req): Json<CreateStorageVolumeRequest>,
) -> Result<Json<ApiResponse<StorageVolumeInfo>>, StatusCode> {
    info!("Creating storage volume: {}", req.name);
    
    let manager = StorageVolumeManager::new(&state.config.storage.base_path);
    
    if let Err(e) = manager.init().await {
        error!("Failed to initialize storage volume manager: {}", e);
        return Ok(Json(ApiResponse::error("Failed to initialize storage".to_string())));
    }
    
    match manager.create_volume(&req.name, req.description, req.labels).await {
        Ok(volume) => Ok(Json(ApiResponse::success(volume))),
        Err(e) => {
            error!("Failed to create storage volume: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Clone a volume from an existing container
pub async fn clone_volume_from_container(
    State(state): State<AppState>,
    Json(req): Json<CloneVolumeRequest>,
) -> Result<Json<ApiResponse<StorageVolumeInfo>>, StatusCode> {
    info!("Cloning volume from container: {}", req.source_container_uuid);
    
    let manager = StorageVolumeManager::new(&state.config.storage.base_path);
    
    if let Err(e) = manager.init().await {
        error!("Failed to initialize storage volume manager: {}", e);
        return Ok(Json(ApiResponse::error("Failed to initialize storage".to_string())));
    }
    
    match manager.create_from_container(&req.source_container_uuid, &req.name, req.description).await {
        Ok(volume) => Ok(Json(ApiResponse::success(volume))),
        Err(e) => {
            error!("Failed to clone volume: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// List all storage volumes
pub async fn list_storage_volumes(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<StorageVolumeInfo>>>, StatusCode> {
    let manager = StorageVolumeManager::new(&state.config.storage.base_path);
    
    if let Err(e) = manager.init().await {
        error!("Failed to initialize storage volume manager: {}", e);
        return Ok(Json(ApiResponse::error("Failed to initialize storage".to_string())));
    }
    
    match manager.list_volumes().await {
        Ok(volumes) => Ok(Json(ApiResponse::success(volumes))),
        Err(e) => {
            error!("Failed to list storage volumes: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Get storage volume info
pub async fn get_storage_volume(
    State(state): State<AppState>,
    Path(volume_id): Path<String>,
) -> Result<Json<ApiResponse<StorageVolumeInfo>>, StatusCode> {
    let manager = StorageVolumeManager::new(&state.config.storage.base_path);
    
    if let Err(e) = manager.init().await {
        error!("Failed to initialize storage volume manager: {}", e);
        return Ok(Json(ApiResponse::error("Failed to initialize storage".to_string())));
    }
    
    match manager.get_volume(&volume_id).await {
        Ok(volume) => Ok(Json(ApiResponse::success(volume))),
        Err(e) => {
            error!("Failed to get storage volume {}: {}", volume_id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Delete a storage volume
pub async fn delete_storage_volume(
    State(state): State<AppState>,
    Path(volume_id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Deleting storage volume: {}", volume_id);
    
    let manager = StorageVolumeManager::new(&state.config.storage.base_path);
    
    if let Err(e) = manager.init().await {
        error!("Failed to initialize storage volume manager: {}", e);
        return Ok(Json(ApiResponse::error("Failed to initialize storage".to_string())));
    }
    
    match manager.delete_volume(&volume_id).await {
        Ok(_) => Ok(Json(ApiResponse::success(format!("Volume {} deleted", volume_id)))),
        Err(e) => {
            error!("Failed to delete storage volume {}: {}", volume_id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}


/// Get current mount info for a container
pub async fn get_container_mount(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<MountInfo>>, StatusCode> {
    info!("Getting mount info for container: {}", container_uuid);
    
    // Check if container exists
    let _container_state = match state.state_manager.get_container(&container_uuid) {
        Some(s) => s,
        None => return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid)))),
    };
    
    // Get tracker data to check current volume
    let tracker = match state.container_tracker.get_container(&container_uuid).await {
        Ok(Some(t)) => t,
        Ok(None) => return Ok(Json(ApiResponse::error("Container tracker data not found".to_string()))),
        Err(e) => return Ok(Json(ApiResponse::error(format!("Failed to load container data: {}", e)))),
    };
    
    // Default volume path is {volumes_path}/{container_uuid}
    let default_volume_path = format!("{}/{}", state.config.storage.volumes_path, container_uuid);
    
    // Check if using custom volume (from attached_volumes or custom mount)
    let current_volume_path = if let Some(custom_vol) = tracker.attached_volumes.iter()
        .find(|v| v.target == "/home/container") 
    {
        custom_vol.source.clone()
    } else {
        default_volume_path.clone()
    };
    
    let is_default = current_volume_path == default_volume_path;
    
    Ok(Json(ApiResponse::success(MountInfo {
        container_uuid,
        current_volume_path,
        is_default_volume: is_default,
    })))
}

/// Swap the /home/container mount to a different volume
/// This will recreate the container with the new volume attached
pub async fn swap_container_mount(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
    Json(req): Json<SwapMountRequest>,
) -> Result<Json<ApiResponse<SwapMountResponse>>, StatusCode> {
    info!("Swapping mount for container {} to volume {}", container_uuid, req.new_volume_id);
    
    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }
    
    // Check if container is locked
    if state.state_manager.is_container_locked(&container_uuid) {
        return Ok(Json(ApiResponse::error("Container is currently locked".to_string())));
    }
    
    // Verify the new volume exists
    let volume_manager = StorageVolumeManager::new(&state.config.storage.base_path);
    if let Err(e) = volume_manager.init().await {
        return Ok(Json(ApiResponse::error(format!("Failed to initialize storage: {}", e))));
    }
    
    let new_volume = match volume_manager.get_volume(&req.new_volume_id).await {
        Ok(v) => v,
        Err(e) => return Ok(Json(ApiResponse::error(format!("Volume not found: {}", e)))),
    };
    
    // Check if volume is already attached to another container
    if let Some(attached_to) = &new_volume.attached_to {
        if attached_to != &container_uuid {
            return Ok(Json(ApiResponse::error(format!(
                "Volume {} is already attached to container {}", 
                req.new_volume_id, attached_to
            ))));
        }
    }
    
    // Get current volume path
    let _tracker = match state.container_tracker.get_container(&container_uuid).await {
        Ok(Some(t)) => t,
        Ok(None) => return Ok(Json(ApiResponse::error("Container tracker data not found".to_string()))),
        Err(e) => return Ok(Json(ApiResponse::error(format!("Failed to load container data: {}", e)))),
    };
    
    let old_volume_path = format!("{}/{}", state.config.storage.volumes_path, container_uuid);
    let new_volume_path = new_volume.path.clone();
    
    // Use lifecycle manager to recreate with new volume
    match state.lifecycle.swap_mount(&container_uuid, &new_volume_path).await {
        Ok(new_container_id) => {
            // Update volume attachment status
            let _ = volume_manager.set_attached(&req.new_volume_id, Some(&container_uuid)).await;
            
            Ok(Json(ApiResponse::success(SwapMountResponse {
                container_uuid: container_uuid.clone(),
                old_volume_path,
                new_volume_path,
                new_container_id,
                message: format!("Mount swapped successfully for container {}", container_uuid),
            })))
        }
        Err(e) => {
            error!("Failed to swap mount for container {}: {}", container_uuid, e);
            Ok(Json(ApiResponse::error(format!("Failed to swap mount: {}", e))))
        }
    }
}

/// Reset container mount to default volume
pub async fn reset_container_mount(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<SwapMountResponse>>, StatusCode> {
    info!("Resetting mount for container {} to default", container_uuid);
    
    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }
    
    // Check if container is locked
    if state.state_manager.is_container_locked(&container_uuid) {
        return Ok(Json(ApiResponse::error("Container is currently locked".to_string())));
    }
    
    // Get current volume info
    let tracker = match state.container_tracker.get_container(&container_uuid).await {
        Ok(Some(t)) => t,
        Ok(None) => return Ok(Json(ApiResponse::error("Container tracker data not found".to_string()))),
        Err(e) => return Ok(Json(ApiResponse::error(format!("Failed to load container data: {}", e)))),
    };
    
    // Get current custom volume path if any
    let old_volume_path = if let Some(custom_vol) = tracker.attached_volumes.iter()
        .find(|v| v.target == "/home/container") 
    {
        custom_vol.source.clone()
    } else {
        format!("{}/{}", state.config.storage.volumes_path, container_uuid)
    };
    
    // Default volume path
    let default_volume_path = format!("{}/{}", state.config.storage.volumes_path, container_uuid);
    
    if old_volume_path == default_volume_path {
        return Ok(Json(ApiResponse::error("Container is already using default volume".to_string())));
    }
    
    // Use lifecycle manager to recreate with default volume
    match state.lifecycle.reset_mount(&container_uuid).await {
        Ok(new_container_id) => {
            Ok(Json(ApiResponse::success(SwapMountResponse {
                container_uuid: container_uuid.clone(),
                old_volume_path,
                new_volume_path: default_volume_path,
                new_container_id,
                message: format!("Mount reset to default for container {}", container_uuid),
            })))
        }
        Err(e) => {
            error!("Failed to reset mount for container {}: {}", container_uuid, e);
            Ok(Json(ApiResponse::error(format!("Failed to reset mount: {}", e))))
        }
    }
}
