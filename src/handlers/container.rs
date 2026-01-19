//! Container handlers - Non-blocking HTTP handlers
//!
//! All handlers use lock-free state manager operations.
//! Power actions are fire-and-forget via async_power manager.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use tracing::{error, info, warn};

use crate::{
    docker::ContainerManager,
    models::{ApiResponse, CreateContainerRequest, SuspendRequest, UnsuspendRequest, ContainerTracker, ResourceLimits, UpdateContainerRequest, UpdateLimitsRequest, InstallationStatus, ContainerLookupResponse, StopRequest, StartRequest},
    types::AppState,
    container_tracker::ContainerTrackingManager,
    state_manager::ContainerState,
    services::PowerCommand,
};

/// Resolve container identifier to (uuid, container_state, docker_container_id)
/// Tries by Docker container ID first, then by UUID
fn resolve_container_full(state: &AppState, identifier: &str) -> Option<(String, ContainerState, String)> {
    // Try by Docker container ID first
    if let Some((uuid, container_state)) = state.state_manager.find_by_container_id(identifier) {
        return Some((uuid, container_state.clone(), identifier.to_string()));
    }
    
    // Try by UUID
    if let Some(container_state) = state.state_manager.get_container(identifier) {
        if let Some(docker_id) = &container_state.container_id {
            return Some((identifier.to_string(), container_state.clone(), docker_id.clone()));
        }
    }
    
    None
}

/// Check if container is suspended (lock-free)
fn check_container_suspended(state: &AppState, container_id: &str) -> Result<bool, String> {
    if let Some((uuid, _)) = state.state_manager.find_by_container_id(container_id) {
        Ok(state.state_manager.is_container_suspended(&uuid))
    } else {
        Err("Container not found in daemon state".to_string())
    }
}

/// Resolve container identifier to container ID (lock-free)
fn resolve_container_id(state: &AppState, identifier: &str) -> Result<String, String> {
    // Try by container ID first
    if state.state_manager.find_by_container_id(identifier).is_some() {
        return Ok(identifier.to_string());
    }
    
    // Try by UUID
    if let Some(container_state) = state.state_manager.get_container(identifier) {
        if let Some(container_id) = &container_state.container_id {
            return Ok(container_id.clone());
        } else {
            return Err("Container ID not found for this UUID".to_string());
        }
    }
    
    Err("Container not found with this identifier".to_string())
}


/// Create a new container
pub async fn create_container(
    State(state): State<AppState>,
    Json(req): Json<CreateContainerRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Creating container with image: {}", req.image);
    
    let custom_uuid = req.custom_uuid.clone()
        .unwrap_or_else(|| ContainerTrackingManager::generate_uuid());
    
    let container_state = ContainerState {
        state: "creating".to_string(),
        container_id: None,
        image: req.image.clone(),
        name: req.name.clone().unwrap_or_else(|| "unnamed".to_string()),
        description: req.description.clone(),
        startup_command: req.startup_command.as_ref().map(|cmd| cmd.join(" ")),
        disk_limit: req.limits.as_ref().and_then(|l| l.disk.clone()),
        memory_limit: req.limits.as_ref().and_then(|l| l.memory.clone()),
        cpu_limit: req.limits.as_ref().and_then(|l| l.cpu.clone()),
        swap_limit: req.limits.as_ref().and_then(|l| l.swap.clone()),
        pids_limit: req.limits.as_ref().and_then(|l| l.pids),
        threads_limit: req.limits.as_ref().and_then(|l| l.threads),
        attached_volumes: req.volumes.as_ref().map(|v| v.iter().map(|vm| vm.source.clone()).collect()).unwrap_or_default(),
        ports: req.ports.clone().unwrap_or_default(),
        env: req.env.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        locked: None,
        lock_reason: None,
        locked_at: None,
        restart_policy: None,
    };

    // Add to state (lock-free)
    if let Err(e) = state.state_manager.add_container(&custom_uuid, container_state).await {
        error!("Failed to add container to daemon state: {}", e);
    }
    
    let manager = ContainerManager::new(state.docker.client().clone());
    let volumes_path = &state.config.storage.volumes_path;
    
    let mut create_req = req.clone();
    create_req.startup_command = Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]);
    
    let startup_cmd = req.startup_command.clone()
        .map(|cmd| {
            if cmd.len() >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh") && cmd[1] == "-c" {
                cmd[2..].join(" ")
            } else {
                cmd.join(" ")
            }
        })
        .unwrap_or_else(|| "sleep infinity".to_string());
    
    let entrypoint_content = if let Some(install_script) = &req.install_content {
        install_script.clone()
    } else {
        startup_cmd.clone()
    };
    
    let data_path = format!("{}/{}_data", volumes_path, custom_uuid);
    if let Err(e) = tokio::fs::create_dir_all(&data_path).await {
        error!("Failed to create data directory: {}", e);
        return Ok(Json(ApiResponse::error(format!("Failed to create data dir: {}", e))));
    }
    let entrypoint_path = format!("{}/entrypoint.sh", data_path);
    if let Err(e) = tokio::fs::write(&entrypoint_path, &entrypoint_content).await {
        error!("Failed to write entrypoint.sh: {}", e);
        return Ok(Json(ApiResponse::error(format!("Failed to write entrypoint: {}", e))));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&entrypoint_path, std::fs::Permissions::from_mode(0o755));
    }
    
    match manager.create_with_networking(create_req, &state.network, &custom_uuid, volumes_path).await {
        Ok((container_id, allocations)) => {
            // Update state (lock-free)
            let _ = state.state_manager.update_container_id(&custom_uuid, &container_id).await;
            let _ = state.state_manager.update_container_state(&custom_uuid, "created").await;

            // Notify remote panel if configured
            if let Some(remote) = &state.remote {
                let remote = remote.clone();
                let uuid = custom_uuid.clone();
                tokio::spawn(async move {
                    remote.send_container_state(&uuid, "created").await;
                });
            }

            let tracker = ContainerTracker {
                custom_uuid: custom_uuid.clone(),
                container_id: container_id.clone(),
                name: req.name.clone().unwrap_or_else(|| "unnamed".to_string()),
                image: req.image.clone(),
                description: req.description.clone(),
                startup_command: req.startup_command.clone(),
                created_at: chrono::Utc::now(),
                limits: req.limits.clone().unwrap_or_default(),
                allocated_ports: allocations.clone(),
                attached_volumes: req.volumes.clone().unwrap_or_default(),
                ports: req.ports.clone().unwrap_or_default(),
                env: req.env.clone(),
                status: "created".to_string(),
                runtime: req.runtime.clone(),
            };
            
            let _ = state.container_tracker.save_container(&tracker).await;

            if let Some(runtime) = req.runtime.as_ref() {
                state.async_power.set_runtime(&container_id, &runtime.start_up, &runtime.stop).await;
            }

            // Spawn single background task for the entire lifecycle
            let bg_state = state.clone();
            let bg_uuid = custom_uuid.clone();
            let bg_container_id = container_id.clone();
            let bg_entrypoint_path = entrypoint_path.clone();
            let bg_startup_cmd = startup_cmd.clone();
            let has_install = req.install_content.is_some();
            
            tokio::spawn(async move {
                let manager = ContainerManager::new(bg_state.docker.client().clone());
                
                // Step 1: Start the container
                info!("Background: Starting container {}", bg_container_id);
                if let Err(e) = manager.start(&bg_container_id).await {
                    error!("Background: Failed to start container {}: {}", bg_container_id, e);
                    let _ = bg_state.state_manager.update_container_state(&bg_uuid, "failed").await;

                    if let Some(remote) = &bg_state.remote {
                        let remote = remote.clone();
                        let uuid = bg_uuid.clone();
                        tokio::spawn(async move { remote.send_container_state(&uuid, "failed").await });
                    }

                    return;
                }
                
                if has_install {
                    // Step 2: If has install script, wait for it to complete
                    let _ = bg_state.state_manager.update_container_state(&bg_uuid, "installing").await;
                    // Notify remote about installing
                    if let Some(remote) = &bg_state.remote {
                        let remote = remote.clone();
                        let uuid = bg_uuid.clone();
                        tokio::spawn(async move { remote.send_install_status(&uuid, "installing", None).await });
                    }

                    let _ = bg_state.state_manager.lock_container(&bg_uuid, "Running install script").await;
                    
                    info!("Background: Waiting for install script to complete for {}", bg_container_id);
                    let docker = bg_state.docker.client().clone();
                    
                    // Wait up to 10 minutes for install to complete
                    let max_wait = 600;
                    let mut waited = 0;
                    
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        waited += 2;
                        
                        if waited > max_wait {
                            error!("Background: Install timed out for {}", bg_container_id);
                            let _ = bg_state.state_manager.update_container_state(&bg_uuid, "install_timeout").await;
                            let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                            break;
                        }
                        
                        match docker.inspect_container(&bg_container_id, None).await {
                            Ok(info) => {
                                if let Some(state) = info.state {
                                    if state.running != Some(true) {
                                        // Container stopped - install finished
                                        let exit_code = state.exit_code.unwrap_or(-1);
                                        info!("Background: Install finished for {} with exit code {}", bg_container_id, exit_code);
                                        
                                        if exit_code == 0 {
                                            // Step 3: Update entrypoint to startup command
                                            info!("Background: Updating entrypoint to startup command for {}", bg_container_id);
                                            if let Err(e) = tokio::fs::write(&bg_entrypoint_path, &bg_startup_cmd).await {
                                                error!("Background: Failed to update entrypoint: {}", e);
                                            }
                                            
                                            // Step 4: Restart container with new entrypoint
                                            info!("Background: Restarting container {} with startup command", bg_container_id);
                                            if let Err(e) = manager.start(&bg_container_id).await {
                                                error!("Background: Failed to restart container: {}", e);
                                                let _ = bg_state.state_manager.update_container_state(&bg_uuid, "failed").await;

                                                if let Some(remote) = &bg_state.remote {
                                                    let remote = remote.clone();
                                                    let uuid = bg_uuid.clone();
                                                    tokio::spawn(async move { remote.send_install_status(&uuid, "install_failed", Some("restart failed")).await });
                                                }

                                            } else {
                                                let _ = bg_state.state_manager.update_container_state(&bg_uuid, "running").await;

                                                if let Some(remote) = &bg_state.remote {
                                                    let remote = remote.clone();
                                                    let uuid = bg_uuid.clone();
                                                    tokio::spawn(async move { remote.send_install_status(&uuid, "install_success", None).await });
                                                }
                                            }
                                        } else {
                                            let _ = bg_state.state_manager.update_container_state(&bg_uuid, "install_failed").await;

                                            if let Some(remote) = &bg_state.remote {
                                                let remote = remote.clone();
                                                let uuid = bg_uuid.clone();
                                                tokio::spawn(async move { remote.send_install_status(&uuid, "install_failed", None).await });
                                            }
                                        }
                                        let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Background: Failed to inspect container {}: {}", bg_container_id, e);
                                let _ = bg_state.state_manager.update_container_state(&bg_uuid, "failed").await;
                                let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                                break;
                            }
                        }
                    }
                } else {
                    // No install script - just mark as running
                    let _ = bg_state.state_manager.update_container_state(&bg_uuid, "running").await;
                }
                
                info!("Background: Container {} lifecycle complete", bg_container_id);
            });

            // Return immediately with "starting" or "installing" state
            let state_str = if has_install { "installing" } else { "starting" };
            let response = serde_json::json!({
                "container_id": container_id,
                "custom_uuid": custom_uuid,
                "name": req.name.clone().unwrap_or_else(|| "unnamed".to_string()),
                "image": req.image,
                "state": state_str,
                "allocated_ports": allocations.iter().map(|alloc| {
                    serde_json::json!({
                        "container_port": alloc.container_port,
                        "host_port": alloc.host_port,
                        "host_ip": alloc.host_ip,
                        "protocol": alloc.protocol
                    })
                }).collect::<Vec<_>>(),
                "limits": tracker.limits
            });
            
            Ok(Json(ApiResponse::success(response)))
        }
        Err(e) => {
            error!("Failed to create container: {}", e);
            let _ = state.state_manager.update_container_state(&custom_uuid, "failed").await;
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}


/// List all containers
pub async fn list_containers(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<crate::models::ContainerInfo>>>, StatusCode> {
    let manager = ContainerManager::new(state.docker.client().clone());
    match manager.list().await {
        Ok(containers) => Ok(Json(ApiResponse::success(containers))),
        Err(e) => {
            error!("Failed to list containers: {}", e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Check if container is locked (installing/updating)
fn check_container_locked(state: &AppState, container_id: &str) -> Option<String> {
    if let Some((_uuid, container_state)) = state.state_manager.find_by_container_id(container_id) {
        if container_state.locked.unwrap_or(false) {
            let reason = container_state.lock_reason.clone()
                .unwrap_or_else(|| "Container is locked".to_string());
            return Some(format!("Container is currently locked: {}", reason));
        }
        // Also check if in installing state
        if container_state.state == "installing" || container_state.state == "updating" {
            return Some(format!("Container is currently {}", container_state.state));
        }
    }
    None
}

/// Start a container (non-blocking, fire and forget)
/// Accepts optional runtime config in request body for startup detection
/// NO LOCKS - power actions never lock containers
pub async fn start_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<StartRequest>>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Received start request for container: {}", id);
    
    // Check if locked (installing/updating) - but NOT for power actions
    if let Some(lock_msg) = check_container_locked(&state, &id) {
        return Ok(Json(ApiResponse::error(lock_msg)));
    }
    
    // Lock-free suspended check
    if let Some((uuid, _)) = state.state_manager.find_by_container_id(&id) {
        if state.state_manager.is_container_suspended(&uuid) {
            return Ok(Json(ApiResponse::error("Cannot start suspended container. Unsuspend it first.".to_string())));
        }
    }
    
    // Get UUID (lock-free)
    let uuid = state.state_manager.get_uuid_for_container_id(&id)
        .unwrap_or_else(|| id.clone());
    
    // ALWAYS load runtime from tracker first (source of truth)
    if let Ok(Some(tracker)) = state.container_tracker.get_container(&uuid).await {
        if let Some(ref runtime) = tracker.runtime {
            info!("Loading runtime from tracker: start_up='{}', stop='{}'", runtime.start_up, runtime.stop);
            state.async_power.set_runtime(&id, &runtime.start_up, &runtime.stop).await;
        }
    }
    
    // Allow request body to override (for testing/manual operations)
    if let Some(Json(req)) = body {
        if let Some(ref runtime) = req.runtime {
            info!("Overriding runtime from request: start_up='{}', stop='{}'", runtime.start_up, runtime.stop);
            state.async_power.set_runtime(&id, &runtime.start_up, &runtime.stop).await;
        }
    }
    
    match state.async_power.start(id.clone(), uuid.clone()).await {
        Ok(_) => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "status": "accepted",
                "message": format!("Start action initiated for container {}", id),
            }))))
        }
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Stop a container (non-blocking, fire and forget)
/// Accepts optional runtime config in request body for graceful stop command
/// NO LOCKS - power actions never lock containers
pub async fn stop_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Received stop request for container: {}", id);
    
    // Check if locked (installing/updating) - but NOT for power actions
    if let Some(lock_msg) = check_container_locked(&state, &id) {
        return Ok(Json(ApiResponse::error(lock_msg)));
    }
    
    let uuid = state.state_manager.get_uuid_for_container_id(&id)
        .unwrap_or_else(|| id.clone());
    
    // ALWAYS load runtime from tracker first (source of truth)
    if let Ok(Some(tracker)) = state.container_tracker.get_container(&uuid).await {
        if let Some(ref runtime) = tracker.runtime {
            info!("Loading runtime from tracker: stop='{}', start_up='{}'", runtime.stop, runtime.start_up);
            state.async_power.set_runtime(&id, &runtime.start_up, &runtime.stop).await;
        }
    }
    
    // Allow request body to override (for testing/manual operations)
    if let Some(Json(req)) = body {
        if let Some(ref runtime) = req.runtime {
            info!("Overriding runtime from request: stop='{}', start_up='{}'", runtime.stop, runtime.start_up);
            state.async_power.set_runtime(&id, &runtime.start_up, &runtime.stop).await;
        }
    }
    
    match state.async_power.stop(id.clone(), uuid.clone()).await {
        Ok(_) => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "status": "accepted",
                "message": format!("Stop action initiated for container {}", id),
            }))))
        }
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Kill a container (non-blocking, fire and forget)
pub async fn kill_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Received kill request for container: {}", id);
    
    // Check if locked (installing/updating)
    if let Some(lock_msg) = check_container_locked(&state, &id) {
        return Ok(Json(ApiResponse::error(lock_msg)));
    }
    
    let uuid = state.state_manager.get_uuid_for_container_id(&id)
        .unwrap_or_else(|| id.clone());
    
    match state.async_power.kill(id.clone(), uuid.clone()).await {
        Ok(_) => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "status": "accepted",
                "message": format!("Kill action initiated for container {}", id),
            }))))
        }
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Restart a container (non-blocking, fire and forget)
pub async fn restart_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Received restart request for container: {}", id);
    
    // Check if locked (installing/updating)
    if let Some(lock_msg) = check_container_locked(&state, &id) {
        return Ok(Json(ApiResponse::error(lock_msg)));
    }
    
    // Lock-free suspended check
    if let Some((uuid, _)) = state.state_manager.find_by_container_id(&id) {
        if state.state_manager.is_container_suspended(&uuid) {
            return Ok(Json(ApiResponse::error("Cannot restart suspended container. Unsuspend it first.".to_string())));
        }
    }
    
    let uuid = state.state_manager.get_uuid_for_container_id(&id)
        .unwrap_or_else(|| id.clone());
    
    // Load runtime config from tracker and set it on async_power
    if let Ok(Some(tracker)) = state.container_tracker.get_container(&uuid).await {
        if let Some(ref runtime) = tracker.runtime {
            state.async_power.set_runtime(&id, &runtime.start_up, &runtime.stop).await;
        }
    }
    
    match state.async_power.restart(id.clone(), uuid.clone()).await {
        Ok(_) => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "status": "accepted",
                "message": format!("Restart action initiated for container {}", id),
            }))))
        }
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Recreate a container with updated configuration (ports, limits, etc.)
/// This follows the Pterodactyl Wings pattern: stop, remove, create, start
/// Container is stateless - UUID persists, container_id changes
pub async fn recreate_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Received recreate request for container: {}", id);
    
    // Check if locked (installing/updating)
    if let Some(lock_msg) = check_container_locked(&state, &id) {
        return Ok(Json(ApiResponse::error(lock_msg)));
    }
    
    // Get UUID for this container (id can be UUID or container_id)
    let uuid = state.state_manager.get_uuid_for_container_id(&id)
        .unwrap_or_else(|| id.clone());
    
    // Spawn background task for recreation using lifecycle manager
    let lifecycle = state.lifecycle.clone();
    let remote = state.remote.clone();
    let uuid_clone = uuid.clone();
    
    // Notify remote panel that recreation is starting
    if let Some(ref remote) = state.remote {
        remote.send_install_status(&uuid, "recreating", Some("Container recreation in progress")).await;
    }
    
    tokio::spawn(async move {
        match lifecycle.recreate_container(&uuid_clone, None).await {
            Ok(new_id) => {
                info!("Container {} recreated successfully, new ID: {}", uuid_clone, new_id);
                // Notify remote panel of success
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_success", Some("Container recreated successfully")).await;
                }
            }
            Err(e) => {
                error!("Container {} recreation failed: {}", uuid_clone, e);
                // Notify remote panel of failure
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_failed", Some(&format!("Recreation failed: {}", e))).await;
                }
            }
        }
    });
    
    Ok(Json(ApiResponse::success(serde_json::json!({
        "status": "accepted",
        "message": format!("Recreate action initiated for container {}", uuid),
    }))))
}

/// Suspend a container (non-blocking)
pub async fn suspend_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SuspendRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let reason = req.message.unwrap_or_else(|| "Container suspended by administrator".to_string());
    info!("Received suspend request for container: {} with reason: {}", id, reason);
    
    // Lock-free check
    let uuid = if let Some((uuid, _)) = state.state_manager.find_by_container_id(&id) {
        if state.state_manager.is_container_suspended(&uuid) {
            return Ok(Json(ApiResponse::error("Container is already suspended".to_string())));
        }
        uuid
    } else {
        id.clone()
    };
    
    match state.power_executor.send(PowerCommand::Suspend {
        container_id: id.clone(),
        uuid: uuid.clone(),
        reason: reason.clone(),
    }) {
        Ok(action_id) => {
            // Update state immediately (lock-free)
            let _ = state.state_manager.suspend_container(&uuid, &reason).await;
            
            // Disable billing for this container
            if let Some(ref monitor) = state.resource_monitor {
                monitor.disable_billing(&uuid);
            }
            
            Ok(Json(ApiResponse::success(serde_json::json!({
                "status": "accepted",
                "action_id": action_id,
                "reason": reason,
                "message": format!("Suspend action sent for container {}", id),
            }))))
        }
        Err(e) => {
            error!("Failed to send suspend command: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Unsuspend a container
pub async fn unsuspend_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UnsuspendRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    let reason = req.message.unwrap_or_else(|| "Container unsuspended by administrator".to_string());
    info!("Unsuspending container: {} with reason: {}", id, reason);
    
    // Lock-free lookup
    if let Some((uuid, _)) = state.state_manager.find_by_container_id(&id) {
        if !state.state_manager.is_container_suspended(&uuid) {
            return Ok(Json(ApiResponse::error("Container is not suspended".to_string())));
        }
        
        // Update state (lock-free)
        let _ = state.state_manager.unsuspend_container(&uuid).await;
        let _ = state.container_tracker.update_container_status(&uuid, "stopped").await;
        
        // Re-enable billing for this container
        if let Some(ref monitor) = state.resource_monitor {
            monitor.enable_billing(&uuid);
        }
        
        Ok(Json(ApiResponse::success(format!("Container {} unsuspended: {}. Container is now stopped and can be started.", id, reason))))
    } else {
        Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())))
    }
}


/// Attach to a container
pub async fn attach_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<crate::models::AttachResponse>>, StatusCode> {
    info!("Creating shell session for container: {}", id);
    
    if let Ok(true) = check_container_suspended(&state, &id) {
        return Ok(Json(ApiResponse::error("Cannot attach to suspended container".to_string())));
    }
    
    let manager = ContainerManager::new(state.docker.client().clone());
    match manager.attach(&id).await {
        Ok(exec_id) => {
            let response = crate::models::AttachResponse {
                exec_id,
                container_id: id,
                status: "shell_session_created".to_string(),
            };
            Ok(Json(ApiResponse::success(response)))
        }
        Err(e) => {
            error!("Failed to create shell session for container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Remove a container
pub async fn remove_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Removing container: {}", id);
    
    let manager = ContainerManager::new(state.docker.client().clone());
    match manager.remove(&id).await {
        Ok(_) => {
            // Release ports
            let mut net_mgr = state.network.write().await;
            net_mgr.release_container_ports(&id);
            drop(net_mgr);
            
            // Remove from state (lock-free)
            if let Some((uuid, _)) = state.state_manager.find_by_container_id(&id) {
                let _ = state.state_manager.remove_container(&uuid).await;
            }
            
            // Remove from tracker
            if let Ok(Some(tracker)) = state.container_tracker.find_by_container_id(&id).await {
                let _ = state.container_tracker.remove_container(&tracker.custom_uuid).await;
            }
            
            Ok(Json(ApiResponse::success(format!("Container {} removed", id))))
        }
        Err(e) => {
            error!("Failed to remove container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Execute a command in a container
pub async fn exec_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<crate::models::ExecRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Executing command in container {}: {:?}", id, req.command);
    
    if let Ok(true) = check_container_suspended(&state, &id) {
        return Ok(Json(ApiResponse::error("Cannot execute commands in suspended container".to_string())));
    }
    
    let manager = ContainerManager::new(state.docker.client().clone());
    let cmd_refs: Vec<&str> = req.command.iter().map(|s| s.as_str()).collect();
    
    match manager.exec_command(&id, cmd_refs).await {
        Ok(output) => Ok(Json(ApiResponse::success(output))),
        Err(e) => {
            error!("Failed to execute command in container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Get container logs
pub async fn get_container_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<crate::models::LogsRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Getting logs for container: {}", id);
    
    let manager = ContainerManager::new(state.docker.client().clone());
    let follow = req.follow.unwrap_or(false);
    let tail = req.tail.as_deref();
    
    match manager.get_logs(&id, follow, tail).await {
        Ok(logs) => Ok(Json(ApiResponse::success(logs))),
        Err(e) => {
            error!("Failed to get logs for container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Get container stats
pub async fn get_container_stats(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Getting stats for container: {}", id);
    
    let manager = ContainerManager::new(state.docker.client().clone());
    match manager.get_stats(&id).await {
        Ok(stats) => Ok(Json(ApiResponse::success(stats))),
        Err(e) => {
            error!("Failed to get stats for container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Debug container issues
pub async fn debug_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Debugging container: {}", id);
    
    let manager = ContainerManager::new(state.docker.client().clone());
    match manager.debug_container(&id).await {
        Ok(debug_info) => Ok(Json(ApiResponse::success(debug_info))),
        Err(e) => {
            error!("Failed to debug container {}: {}", id, e);
            Ok(Json(ApiResponse::error(e.to_string())))
        }
    }
}

/// Update a container with new update script
pub async fn update_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateContainerRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Updating container: {}", id);
    
    if let Ok(true) = check_container_suspended(&state, &id) {
        return Ok(Json(ApiResponse::error("Cannot update suspended container".to_string())));
    }
    
    // Lock-free lookup - try by Docker ID first, then by UUID
    if let Some((uuid, container_state, docker_id)) = resolve_container_full(&state, &id) {
        if container_state.locked.unwrap_or(false) {
            return Ok(Json(ApiResponse::error("Container is currently locked (installing/updating)".to_string())));
        }
        
        // Lock and update state immediately
        let _ = state.state_manager.lock_container(&uuid, "Running update script").await;
        let _ = state.state_manager.update_container_state(&uuid, "updating").await;
        
        let volumes_path = state.config.storage.volumes_path.clone();
        let startup_cmd = container_state.startup_command.clone()
            .unwrap_or_else(|| "sleep infinity".to_string());
        let update_script = req.update_content.clone();
        
        // Spawn entire update lifecycle in background
        let bg_state = state.clone();
        let bg_uuid = uuid.clone();
        let bg_docker_id = docker_id.clone();
        
        tokio::spawn(async move {
            let docker = bg_state.docker.client().clone();
            let manager = ContainerManager::new(docker.clone());
            let entrypoint_path = format!("{}/{}_data/entrypoint.sh", volumes_path, bg_uuid);
            
            // Step 1: Stop container if running
            info!("Stopping container {} for update", bg_docker_id);
            let _ = manager.stop(&bg_docker_id).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            
            // Step 2: Write update script to entrypoint.sh
            info!("Writing update script to {}", entrypoint_path);
            if let Err(e) = tokio::fs::write(&entrypoint_path, &update_script).await {
                error!("Failed to write entrypoint.sh: {}", e);
                let _ = bg_state.state_manager.update_container_state(&bg_uuid, "update_failed").await;
                let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                return;
            }
            
            // Step 3: Start container (runs update script via entrypoint)
            info!("Starting container {} to run update script", bg_docker_id);
            if let Err(e) = manager.start(&bg_docker_id).await {
                error!("Failed to start container for update: {}", e);
                let _ = bg_state.state_manager.update_container_state(&bg_uuid, "update_failed").await;
                let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                return;
            }
            
            // Step 4: Wait for update to complete
            let max_wait = 600;
            let mut waited = 0;
            
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                waited += 2;
                
                if waited > max_wait {
                    error!("Update timed out for {}", bg_docker_id);
                    let _ = bg_state.state_manager.update_container_state(&bg_uuid, "update_timeout").await;
                    let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                    break;
                }
                
                match docker.inspect_container(&bg_docker_id, None).await {
                    Ok(info) => {
                        if let Some(state) = info.state {
                            if state.running != Some(true) {
                                let exit_code = state.exit_code.unwrap_or(-1);
                                info!("Update finished for {} with exit code {}", bg_docker_id, exit_code);
                                
                                if exit_code == 0 {
                                    // Step 5: Update entrypoint to startup command
                                    if let Err(e) = tokio::fs::write(&entrypoint_path, &startup_cmd).await {
                                        error!("Failed to update entrypoint: {}", e);
                                    }
                                    
                                    // Step 6: Restart container
                                    if let Err(e) = manager.start(&bg_docker_id).await {
                                        error!("Failed to restart container: {}", e);
                                        let _ = bg_state.state_manager.update_container_state(&bg_uuid, "failed").await;
                                    } else {
                                        let _ = bg_state.state_manager.update_container_state(&bg_uuid, "running").await;
                                    }
                                } else {
                                    let _ = bg_state.state_manager.update_container_state(&bg_uuid, "update_failed").await;
                                }
                                let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to inspect container {}: {}", bg_docker_id, e);
                        let _ = bg_state.state_manager.update_container_state(&bg_uuid, "failed").await;
                        let _ = bg_state.state_manager.unlock_container(&bg_uuid).await;
                        break;
                    }
                }
            }
        });
        
        // Return immediately
        Ok(Json(ApiResponse::success(format!("Update started for container {}", id))))
    } else {
        Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())))
    }
}

/// Run installation script in container (uses single container approach)
/// Returns immediately - panel is notified via callback when complete
pub async fn install_container(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateContainerRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Running installation script for container: {}", id);
    
    if let Ok(true) = check_container_suspended(&state, &id) {
        return Ok(Json(ApiResponse::error("Cannot install on suspended container".to_string())));
    }
    
    // Lock-free lookup - try by Docker ID first, then by UUID
    if let Some((uuid, container_state, _docker_id)) = resolve_container_full(&state, &id) {
        if container_state.locked.unwrap_or(false) {
            return Ok(Json(ApiResponse::error("Container is currently locked (installing/updating)".to_string())));
        }
        
        let install_script = req.update_content.clone();
        let image = req.image.clone();
        let startup_command = req.startup_command.clone();
        
        // Spawn reinstall in background with callback to panel
        let lifecycle = state.lifecycle.clone();
        let remote = state.remote.clone();
        let uuid_clone = uuid.clone();
        
        tokio::spawn(async move {
            info!("Background: Starting reinstall for {}", uuid_clone);
            
            // Notify panel that install is starting
            if let Some(ref remote) = remote {
                remote.send_install_status(&uuid_clone, "installing", None).await;
            }
            
            match lifecycle.reinstall(&uuid_clone, &install_script, image.as_deref(), startup_command.as_ref()).await {
                Ok(new_id) => {
                    info!("Background: Reinstall complete for {}, new container: {}", uuid_clone, new_id);
                    // Notify panel of success
                    if let Some(ref remote) = remote {
                        remote.send_install_status(&uuid_clone, "install_success", None).await;
                    }
                }
                Err(e) => {
                    error!("Background: Reinstall failed for {}: {}", uuid_clone, e);
                    // Notify panel of failure
                    if let Some(ref remote) = remote {
                        remote.send_install_status(&uuid_clone, "install_failed", Some(&e)).await;
                    }
                }
            }
        });
        
        // Return immediately - panel will be notified via callback
        Ok(Json(ApiResponse::success(serde_json::json!({
            "message": format!("Installation started for container {}", id),
            "uuid": uuid,
            "status": "installing"
        }))))
    } else {
        Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())))
    }
}



/// Update container resource limits
pub async fn update_container_limits(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateLimitsRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Updating limits for container: {}", id);
    
    if let Ok(true) = check_container_suspended(&state, &id) {
        return Ok(Json(ApiResponse::error("Cannot update limits of suspended container".to_string())));
    }
    
    // Lock-free lookup
    if let Some((uuid, container_state)) = state.state_manager.find_by_container_id(&id) {
        if container_state.locked.unwrap_or(false) {
            return Ok(Json(ApiResponse::error("Container is currently locked (installing/updating)".to_string())));
        }
        
        let restart = req.restart_container.unwrap_or(false);
        let manager = ContainerManager::new(state.docker.client().clone());
        
        match manager.update_limits(&id, &req.limits, restart).await {
            Ok(result) => {
                let _ = state.state_manager.update_container_limits(&uuid, &req.limits).await;
                
                if let Ok(Some(mut tracker)) = state.container_tracker.find_by_container_id(&id).await {
                    tracker.limits = req.limits.clone();
                    let _ = state.container_tracker.save_container(&tracker).await;
                }
                
                Ok(Json(ApiResponse::success(result)))
            }
            Err(e) => {
                error!("Failed to update limits for container {}: {}", id, e);
                Ok(Json(ApiResponse::error(e.to_string())))
            }
        }
    } else {
        Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())))
    }
}

/// Get container installation/update status
/// Returns current container_id which may change during reinstall
pub async fn get_container_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<InstallationStatus>>, StatusCode> {
    info!("Getting status for container: {}", id);
    
    // Lock-free lookup - try by container ID first, then by UUID
    if let Some((uuid, container_state)) = state.state_manager.find_by_container_id(&id) {
        let status = InstallationStatus {
            status: container_state.state.clone(),
            progress: container_state.lock_reason.clone(),
            logs: None,
            container_id: container_state.container_id.clone(),
        };
        Ok(Json(ApiResponse::success(status)))
    } else if let Some(container_state) = state.state_manager.get_container(&id) {
        // Try as UUID
        let status = InstallationStatus {
            status: container_state.state.clone(),
            progress: container_state.lock_reason.clone(),
            logs: None,
            container_id: container_state.container_id.clone(),
        };
        Ok(Json(ApiResponse::success(status)))
    } else {
        Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())))
    }
}

/// Get container ID from UUID
pub async fn get_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<ApiResponse<ContainerLookupResponse>>, StatusCode> {
    info!("Looking up container by UUID: {}", uuid);
    
    // Lock-free lookup
    if let Some(container_state) = state.state_manager.get_container(&uuid) {
        if let Some(container_id) = &container_state.container_id {
            let container_id_clone = container_id.clone();
            let name = container_state.name.clone();
            let image = container_state.image.clone();
            let cached_state = container_state.state.clone();

            let special_states = ["install_failed", "installing", "suspended", "creating", "failed"];
            let actual_state = if special_states.contains(&cached_state.as_str()) {
                cached_state
            } else {
                let pending_action = state.async_power.has_pending_action(&container_id_clone).await;

                // Query Docker with timeout
                let docker_state = match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    state.docker.client().inspect_container(&container_id_clone, None)
                ).await {
                    Ok(Ok(info)) => info.state,
                    Ok(Err(_)) => None,
                    Err(_) => None,
                };

                if let Some(action) = pending_action {
                    match action.as_str() {
                        "start" => "starting".to_string(),
                        "stop" | "kill" => "stopping".to_string(),
                        "restart" => {
                            if docker_state.as_ref().and_then(|s| s.running).unwrap_or(false) {
                                "stopping".to_string()
                            } else {
                                "starting".to_string()
                            }
                        }
                        _ => "unknown".to_string(),
                    }
                } else if let Some(docker_state) = docker_state {
                    if docker_state.oom_killed == Some(true) {
                        "oom_killed".to_string()
                    } else if docker_state.running == Some(true) {
                        "running".to_string()
                    } else if docker_state.paused == Some(true) {
                        "paused".to_string()
                    } else if docker_state.restarting == Some(true) {
                        "restarting".to_string()
                    } else if docker_state.dead == Some(true) {
                        "dead".to_string()
                    } else if docker_state.exit_code == Some(137) {
                        "oom_killed".to_string()
                    } else {
                        "stopped".to_string()
                    }
                } else {
                    "unknown".to_string()
                }
            };
            
            let response = ContainerLookupResponse {
                uuid: uuid.clone(),
                container_id: container_id_clone,
                name,
                state: actual_state,
                image,
                locked: container_state.locked,
                lock_reason: container_state.lock_reason.clone(),
            };
            Ok(Json(ApiResponse::success(response)))
        } else {
            Ok(Json(ApiResponse::error("Container ID not found for this UUID".to_string())))
        }
    } else {
        Ok(Json(ApiResponse::error("Container not found with this UUID".to_string())))
    }
}

/// Start a container by UUID
pub async fn start_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    body: Option<Json<StartRequest>>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Starting container by UUID: {}", uuid);
    match resolve_container_id(&state, &uuid) {
        Ok(container_id) => start_container(State(state), Path(container_id), body).await,
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Stop a container by UUID
pub async fn stop_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Stopping container by UUID: {}", uuid);
    match resolve_container_id(&state, &uuid) {
        Ok(container_id) => stop_container(State(state), Path(container_id), body).await,
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Restart a container by UUID
pub async fn restart_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Restarting container by UUID: {}", uuid);
    match resolve_container_id(&state, &uuid) {
        Ok(container_id) => restart_container(State(state), Path(container_id)).await,
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Suspend a container by UUID
pub async fn suspend_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(req): Json<SuspendRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Suspending container by UUID: {}", uuid);
    match resolve_container_id(&state, &uuid) {
        Ok(container_id) => suspend_container(State(state), Path(container_id), Json(req)).await,
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Unsuspend a container by UUID
pub async fn unsuspend_container_by_uuid(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(req): Json<UnsuspendRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Unsuspending container by UUID: {}", uuid);
    match resolve_container_id(&state, &uuid) {
        Ok(container_id) => unsuspend_container(State(state), Path(container_id), Json(req)).await,
        Err(e) => Ok(Json(ApiResponse::error(e))),
    }
}

/// Get power action status by action ID
pub async fn get_power_action_status(
    State(state): State<AppState>,
    Path(action_id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    // Lock-free lookup
    match state.power_actions.get_action_status(&action_id) {
        Some(result) => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "action_id": result.action_id,
                "container_id": result.container_id,
                "container_uuid": result.container_uuid,
                "action": format!("{}", result.action),
                "status": result.status,
                "message": result.message,
                "started_at": result.started_at.to_rfc3339(),
                "completed_at": result.completed_at.map(|t| t.to_rfc3339()),
            }))))
        }
        None => Ok(Json(ApiResponse::error(format!("Action {} not found", action_id)))),
    }
}

/// Check if a container has a pending power action
pub async fn get_container_pending_action(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    // Lock-free lookup
    match state.power_actions.has_pending_action(&id) {
        Some(action_id) => {
            let status = state.power_actions.get_action_status(&action_id);
            Ok(Json(ApiResponse::success(serde_json::json!({
                "has_pending_action": true,
                "action_id": action_id,
                "status": status.map(|s| format!("{:?}", s.status)),
            }))))
        }
        None => {
            Ok(Json(ApiResponse::success(serde_json::json!({
                "has_pending_action": false,
            }))))
        }
    }
}

/// Update container configuration (env, limits, ports, image)
/// This triggers a container recreation - container is stateless
/// UUID persists, container_id changes, volume persists
pub async fn update_container_config(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(req): Json<crate::models::UpdateConfigRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Updating container config for UUID: {}", uuid);
    
    // Check if locked
    if state.state_manager.is_container_locked(&uuid) {
        return Ok(Json(ApiResponse::error("Container is currently locked (operation in progress)".to_string())));
    }
    
    // Check if suspended
    if state.state_manager.is_container_suspended(&uuid) {
        return Ok(Json(ApiResponse::error("Cannot update suspended container".to_string())));
    }

    // Fast-path: startup command update ONLY.
    // Requirement: startup command changes must only edit entrypoint.sh and daemon state,
    // without mutating Docker CMD/env or recreating the container.
    let startup_only = req.startup_command.is_some()
        && req.env.is_none()
        && req.limits.is_none()
        && req.ports.is_none()
        && req.image.is_none();

    if startup_only {
        // Ensure container exists in daemon state
        if state.state_manager.get_container(&uuid).is_none() {
            return Ok(Json(ApiResponse::error("Container not found in daemon state".to_string())));
        }

        let startup_vec = req.startup_command.clone().unwrap_or_default();
        let startup_cmd = if startup_vec.len() >= 3
            && (startup_vec[0] == "sh" || startup_vec[0] == "/bin/sh")
            && startup_vec[1] == "-c"
        {
            startup_vec[2..].join(" ")
        } else {
            startup_vec.join(" ")
        };

        let volumes_path = state.config.storage.volumes_path.clone();
        let data_path = format!("{}/{}_data", volumes_path, uuid);
        if let Err(e) = tokio::fs::create_dir_all(&data_path).await {
            error!("Failed to create data directory for {}: {}", uuid, e);
            return Ok(Json(ApiResponse::error(format!(
                "Failed to create data directory: {}",
                e
            ))));
        }

        let entrypoint_path = format!("{}/entrypoint.sh", data_path);
        if let Err(e) = tokio::fs::write(&entrypoint_path, format!("{}\n", startup_cmd)).await {
            error!("Failed to write entrypoint.sh for {}: {}", uuid, e);
            return Ok(Json(ApiResponse::error(format!("Failed to write entrypoint: {}", e))));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&entrypoint_path, std::fs::Permissions::from_mode(0o755));
        }

        let _ = state
            .state_manager
            .update_container_startup_command(&uuid, Some(startup_cmd.clone()))
            .await;
        let _ = state
            .container_tracker
            .update_container_startup_command(&uuid, req.startup_command.clone())
            .await;

        return Ok(Json(ApiResponse::success(serde_json::json!({
            "status": "ok",
            "message": "Startup command updated. Restart the server for changes to take effect.",
            "startupCommand": startup_cmd,
        }))));
    }
    
    // Build update request
    let update = crate::services::ContainerUpdateRequest {
        env: req.env,
        limits: req.limits,
        ports: req.ports,
        image: req.image,
        startup_command: req.startup_command,
    };
    
    // Spawn background task for recreation
    let lifecycle = state.lifecycle.clone();
    let remote = state.remote.clone();
    let uuid_clone = uuid.clone();
    
    // Notify remote panel that config update is starting
    if let Some(ref remote) = state.remote {
        remote.send_install_status(&uuid, "recreating", Some("Applying configuration changes")).await;
    }
    
    tokio::spawn(async move {
        match lifecycle.recreate_container(&uuid_clone, Some(update)).await {
            Ok(new_id) => {
                info!("Container {} config updated, new ID: {}", uuid_clone, new_id);
                // Notify remote panel of success
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_success", Some("Configuration updated successfully")).await;
                }
            }
            Err(e) => {
                error!("Container {} config update failed: {}", uuid_clone, e);
                // Notify remote panel of failure
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_failed", Some(&format!("Config update failed: {}", e))).await;
                }
            }
        }
    });
    
    Ok(Json(ApiResponse::success(serde_json::json!({
        "status": "accepted",
        "message": format!("Config update initiated for container {}", uuid),
    }))))
}

/// Update container environment variables only (triggers recreation)
pub async fn update_container_env(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(req): Json<crate::models::UpdateEnvRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Updating container env for UUID: {}", uuid);
    
    // Check if locked
    if state.state_manager.is_container_locked(&uuid) {
        return Ok(Json(ApiResponse::error("Container is currently locked".to_string())));
    }
    
    // Spawn background task
    let lifecycle = state.lifecycle.clone();
    let uuid_clone = uuid.clone();
    let env = req.env;
    
    tokio::spawn(async move {
        match lifecycle.update_env(&uuid_clone, env).await {
            Ok(new_id) => {
                info!("Container {} env updated, new ID: {}", uuid_clone, new_id);
            }
            Err(e) => {
                error!("Container {} env update failed: {}", uuid_clone, e);
            }
        }
    });
    
    Ok(Json(ApiResponse::success(serde_json::json!({
        "status": "accepted",
        "message": format!("Env update initiated for container {}", uuid),
    }))))
}

/// Force unlock a container (admin operation to clear stuck locks)
pub async fn force_unlock_container(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Force unlocking container: {}", uuid);
    
    // Check if container exists
    if state.state_manager.get_container(&uuid).is_none() {
        return Ok(Json(ApiResponse::error("Container not found".to_string())));
    }
    
    // Force unlock
    if let Err(e) = state.state_manager.unlock_container(&uuid).await {
        error!("Failed to force unlock container {}: {}", uuid, e);
        return Ok(Json(ApiResponse::error(format!("Failed to unlock: {}", e))));
    }
    
    info!("Container {} force unlocked successfully", uuid);
    
    Ok(Json(ApiResponse::success(serde_json::json!({
        "status": "ok",
        "message": format!("Container {} unlocked", uuid),
    }))))
}


/// Reinstall a container - proper Pterodactyl-style flow
/// 
/// Flow:
/// 1. Callback → INSTALLING
/// 2. Stop and remove old container
/// 3. Pull image
/// 4. Create new container with bindings
/// 5. Mount volume, run install script
/// 6. After install exits → update entrypoint to startup command
/// 7. Callback → READY
/// 
/// Panel is source of truth - Lightd just executes and reports back.
/// This is SYNCHRONOUS - creates new container and returns new ID immediately.
/// Install script runs in background after container is created.
pub async fn reinstall_container(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(req): Json<crate::models::ReinstallRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    info!("Reinstall request for container: {}", uuid);
    
    // Check if container exists
    let container_state = match state.state_manager.get_container(&uuid) {
        Some(cs) => cs,
        None => return Ok(Json(ApiResponse::error("Container not found".to_string()))),
    };
    
    // Check if locked
    if container_state.locked.unwrap_or(false) {
        return Ok(Json(ApiResponse::error("Container is currently locked".to_string())));
    }
    
    // Check if suspended
    if state.state_manager.is_container_suspended(&uuid) {
        return Ok(Json(ApiResponse::error("Cannot reinstall suspended container".to_string())));
    }
    
    // Lock immediately
    if let Err(e) = state.state_manager.lock_container(&uuid, "Reinstalling").await {
        return Ok(Json(ApiResponse::error(format!("Failed to lock: {}", e))));
    }
    
    // Set state to installing
    state.state_manager.update_container_state(&uuid, "installing").await.ok();
    
    // Notify remote panel that container is locked for reinstall
    if let Some(ref remote) = state.remote {
        remote.send_install_status(&uuid, "installing", Some("Reinstalling container")).await;
    }
    
    // SYNCHRONOUS: Create new container and get new ID
    let new_container_id = match create_reinstall_container(
        &uuid,
        &req,
        &state.docker,
        &state.network,
        &state.state_manager,
        &state.container_tracker,
        &state.config.storage.volumes_path,
    ).await {
        Ok(id) => id,
        Err(e) => {
            error!("Reinstall failed for {}: {}", uuid, e);
            state.state_manager.update_container_state(&uuid, "install_failed").await.ok();
            state.state_manager.unlock_container(&uuid).await.ok();
            return Ok(Json(ApiResponse::error(e)));
        }
    };
    
    info!("Reinstall: New container created: {} for {}", new_container_id, uuid);
    
    // Spawn background task to run install script and startup
    let remote = state.remote.clone();
    let state_manager = state.state_manager.clone();
    let docker = state.docker.clone();
    let uuid_clone = uuid.clone();
    let new_cid = new_container_id.clone();
    let startup_command = req.startup_command.clone();
    let volumes_path = state.config.storage.volumes_path.clone();
    
    tokio::spawn(async move {
        // Run install script and startup in background
        let result = run_install_and_startup(
            &uuid_clone,
            &new_cid,
            &startup_command,
            &docker,
            &state_manager,
            &volumes_path,
        ).await;
        
        match result {
            Ok(_) => {
                info!("Reinstall complete for {}", uuid_clone);
                state_manager.unlock_container(&uuid_clone).await.ok();
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_success", None).await;
                }
            }
            Err(e) => {
                error!("Reinstall install phase failed for {}: {}", uuid_clone, e);
                state_manager.unlock_container(&uuid_clone).await.ok();
                if let Some(ref remote) = remote {
                    remote.send_install_status(&uuid_clone, "install_failed", Some(&e)).await;
                }
                state_manager.update_container_state(&uuid_clone, "install_failed").await.ok();
                state_manager.unlock_container(&uuid_clone).await.ok();
            }
        }
    });
    
    // Return immediately with new container ID
    Ok(Json(ApiResponse::success(serde_json::json!({
        "status": "installing",
        "message": format!("Reinstall started for container {}", uuid),
        "uuid": uuid,
        "new_container_id": new_container_id,
    }))))
}

/// Create new container for reinstall (synchronous part)
/// Returns new container ID immediately
async fn create_reinstall_container(
    uuid: &str,
    req: &crate::models::ReinstallRequest,
    docker: &std::sync::Arc<crate::docker::DockerClient>,
    network: &std::sync::Arc<tokio::sync::RwLock<crate::docker::NetworkManager>>,
    state_manager: &std::sync::Arc<crate::state_manager::StateManager>,
    container_tracker: &std::sync::Arc<crate::container_tracker::ContainerTrackingManager>,
    volumes_path: &str,
) -> Result<String, String> {
    let manager = ContainerManager::new(docker.client.clone());
    
    // Load existing tracker for port allocations and other config
    let tracker = container_tracker.get_container(uuid).await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Container tracker not found".to_string())?;
    
    let old_container_id = tracker.container_id.clone();
    
    // Step 1: Stop old container
    info!("Reinstall: Stopping old container {}", old_container_id);
    let _ = manager.stop(&old_container_id).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Step 2: Remove old container
    info!("Reinstall: Removing old container {}", old_container_id);
    let _ = manager.remove(&old_container_id).await;
    
    // Step 3: Release old ports
    {
        let mut net_mgr = network.write().await;
        for alloc in &tracker.allocated_ports {
            if let Ok(port) = alloc.host_port.parse::<u16>() {
                net_mgr.release_port(port);
            }
        }
    }
    
    // Step 4: Pull image
    info!("Reinstall: Pulling image {}", req.image);
    manager.pull_image_if_needed(&req.image).await
        .map_err(|e| format!("Failed to pull image: {}", e))?;
    
    // Step 5: Setup paths and write install script
    let data_path = format!("{}/{}_data", volumes_path, uuid);
    let entrypoint_path = format!("{}/entrypoint.sh", data_path);
    
    tokio::fs::create_dir_all(&data_path).await
        .map_err(|e| format!("Failed to create data dir: {}", e))?;
    
    // Write install script to entrypoint
    tokio::fs::write(&entrypoint_path, &req.install_script).await
        .map_err(|e| format!("Failed to write install script: {}", e))?;
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&entrypoint_path, std::fs::Permissions::from_mode(0o755));
    }
    
    // Step 6: Build port map (reuse existing allocations)
    let ports_map: std::collections::HashMap<String, String> = tracker.allocated_ports
        .iter()
        .map(|p| (p.container_port.clone(), p.host_port.clone()))
        .collect();
    
    // Step 7: Create new container
    info!("Reinstall: Creating new container for {}", uuid);
    let create_req = crate::models::CreateContainerRequest {
        image: req.image.clone(),
        name: Some(tracker.name.clone()),
        description: tracker.description.clone(),
        startup_command: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
        env: req.env.clone().or(tracker.env.clone()),
        ports: if ports_map.is_empty() { req.ports.clone() } else { Some(ports_map) },
        volumes: None,
        command: None,
        working_dir: None,
        restart_policy: None,
        custom_uuid: Some(uuid.to_string()),
        limits: req.limits.clone().or(Some(tracker.limits.clone())),
        install_content: None,
        runtime: req.runtime.clone(),
    };
    
    let (new_container_id, allocations) = manager.create_with_networking(
        create_req,
        network,
        uuid,
        volumes_path,
    ).await.map_err(|e| format!("Failed to create container: {}", e))?;
    
    // Step 8: Update tracker with new container ID
    let mut updated_tracker = tracker.clone();
    updated_tracker.container_id = new_container_id.clone();
    updated_tracker.allocated_ports = allocations;
    updated_tracker.image = req.image.clone();
    updated_tracker.startup_command = Some(req.startup_command.clone());
    updated_tracker.runtime = req.runtime.clone();
    container_tracker.save_container(&updated_tracker).await.ok();
    
    // Update state manager with new container ID
    state_manager.update_container_id(uuid, &new_container_id).await.ok();
    
    Ok(new_container_id)
}

/// Run install script and startup (background part)
async fn run_install_and_startup(
    uuid: &str,
    container_id: &str,
    startup_command: &[String],
    docker: &std::sync::Arc<crate::docker::DockerClient>,
    state_manager: &std::sync::Arc<crate::state_manager::StateManager>,
    volumes_path: &str,
) -> Result<(), String> {
    let manager = ContainerManager::new(docker.client.clone());
    
    // Start container (runs install script)
    info!("Reinstall: Starting container to run install script");
    manager.start(container_id).await
        .map_err(|e| format!("Failed to start container: {}", e))?;
    
    // Wait for install to complete (max 10 min)
    let install_success = wait_for_container_exit(&docker.client, container_id, 600).await;
    
    if !install_success {
        return Err("Install script failed or timed out".to_string());
    }
    
    // Write startup command to entrypoint
    info!("Reinstall: Install complete, writing startup command");
    let data_path = format!("{}/{}_data", volumes_path, uuid);
    let entrypoint_path = format!("{}/entrypoint.sh", data_path);
    
    // Extract actual command from ["sh", "-c", "actual command"]
    let startup_cmd = if startup_command.len() >= 3 
        && (startup_command[0] == "sh" || startup_command[0] == "/bin/sh") 
        && startup_command[1] == "-c" 
    {
        startup_command[2..].join(" ")
    } else {
        startup_command.join(" ")
    };
    
    tokio::fs::write(&entrypoint_path, &startup_cmd).await
        .map_err(|e| format!("Failed to write startup command: {}", e))?;
    
    // Start container with startup command
    info!("Reinstall: Starting container with startup command");
    if let Err(e) = manager.start(container_id).await {
        warn!("Reinstall: Failed to start with startup command: {}", e);
        state_manager.update_container_state(uuid, "stopped").await.ok();
    } else {
        state_manager.update_container_state(uuid, "running").await.ok();
    }
    
    Ok(())
}

/// Execute the actual reinstall process (legacy - kept for reference)
#[allow(dead_code)]
async fn execute_reinstall(
    uuid: &str,
    req: &crate::models::ReinstallRequest,
    docker: &std::sync::Arc<crate::docker::DockerClient>,
    network: &std::sync::Arc<tokio::sync::RwLock<crate::docker::NetworkManager>>,
    state_manager: &std::sync::Arc<crate::state_manager::StateManager>,
    container_tracker: &std::sync::Arc<crate::container_tracker::ContainerTrackingManager>,
    volumes_path: &str,
) -> Result<String, String> {
    let new_container_id = create_reinstall_container(uuid, req, docker, network, state_manager, container_tracker, volumes_path).await?;
    run_install_and_startup(uuid, &new_container_id, &req.startup_command, docker, state_manager, volumes_path).await?;
    Ok(new_container_id)
}

/// Wait for container to exit (install script completion)
async fn wait_for_container_exit(docker: &bollard::Docker, container_id: &str, timeout_secs: u64) -> bool {
    let mut waited = 0u64;
    
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        waited += 2;
        
        if waited > timeout_secs {
            error!("Container {} timed out after {}s", container_id, timeout_secs);
            return false;
        }
        
        match docker.inspect_container(container_id, None).await {
            Ok(info) => {
                if let Some(state) = info.state {
                    if state.running != Some(true) {
                        let exit_code = state.exit_code.unwrap_or(-1);
                        info!("Container exited with code {}", exit_code);
                        return exit_code == 0;
                    }
                }
            }
            Err(e) => {
                error!("Failed to inspect container: {}", e);
                return false;
            }
        }
    }
}
