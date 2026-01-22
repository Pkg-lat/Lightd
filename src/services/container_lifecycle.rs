//! Container Lifecycle Manager - Stateless container recreation
//!
//! Containers are disposable shells around persistent volumes.
//! UUID is the permanent identity, Docker container ID is ephemeral.
//!
//! Key principles:
//! - UUID never changes, container_id can change anytime
//! - Volume named by UUID persists across container recreations
//! - All config changes (env, limits, ports) trigger container recreation
//! - Lock during transitions to prevent concurrent operations
//! - Async, non-blocking - all operations fire-and-forget

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error, warn};

use crate::docker::{ContainerManager, NetworkManager};
use crate::models::{CreateContainerRequest, ResourceLimits};
use crate::container_tracker::ContainerTrackingManager;
use crate::state_manager::StateManager;
use crate::docker::network::PortAllocation;

/// Request to update container configuration
#[derive(Debug, Clone)]
pub struct ContainerUpdateRequest {
    pub env: Option<HashMap<String, String>>,
    pub limits: Option<ResourceLimits>,
    pub ports: Option<HashMap<String, String>>,
    pub image: Option<String>,
    pub startup_command: Option<Vec<String>>,
}

/// Container lifecycle manager - handles stateless container recreation
pub struct ContainerLifecycleManager {
    docker: Arc<bollard::Docker>,
    state_manager: Arc<StateManager>,
    container_tracker: Arc<ContainerTrackingManager>,
    network: Arc<RwLock<NetworkManager>>,
    volumes_path: String,
    remote: Option<Arc<crate::remote::Remote>>,
}

impl ContainerLifecycleManager {
    pub fn new(
        docker: Arc<bollard::Docker>,
        state_manager: Arc<StateManager>,
        container_tracker: Arc<ContainerTrackingManager>,
        network: Arc<RwLock<NetworkManager>>,
        volumes_path: String,
        remote: Option<Arc<crate::remote::Remote>>,
    ) -> Self {
        Self {
            docker,
            state_manager,
            container_tracker,
            network,
            volumes_path,
            remote,
        }
    }

    /// Recreate container with updated configuration
    /// This is the core operation - destroy old container, create new one with same UUID
    /// Volume persists, container_id changes
    pub async fn recreate_container(
        &self,
        uuid: &str,
        update: Option<ContainerUpdateRequest>,
    ) -> Result<String, String> {
        info!("Lifecycle: Starting container recreation for UUID {}", uuid);

        // 1. Lock the container
        self.state_manager.lock_container(uuid, "Recreating container").await
            .map_err(|e| format!("Failed to lock container: {}", e))?;
        self.state_manager.update_container_state(uuid, "recreating").await
            .map_err(|e| format!("Failed to update state: {}", e))?;

        // Notify remote panel that server is locked during recreation
        if let Some(ref remote) = self.remote {
            let request_id = self.state_manager.get_container(uuid).and_then(|s| s.operation_id);
            remote
                .send_install_status_with_container_id_and_id(
                    uuid,
                    "recreating",
                    Some("Container recreation in progress"),
                    None,
                    request_id.as_deref(),
                )
                .await;
        }

        // 2. Load current tracker data
        let tracker = match self.container_tracker.get_container(uuid).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                self.unlock_container(uuid).await;
                return Err("Container tracker data not found".to_string());
            }
            Err(e) => {
                self.unlock_container(uuid).await;
                return Err(format!("Failed to load container data: {}", e));
            }
        };

        let old_container_id = tracker.container_id.clone();
        let manager = ContainerManager::new(self.docker.as_ref().clone());

        // 3. Stop and remove old container (ignore errors - might not exist)
        info!("Lifecycle: Stopping old container {}", old_container_id);
        let _ = manager.stop(&old_container_id).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        info!("Lifecycle: Removing old container {}", old_container_id);
        if let Err(e) = manager.remove(&old_container_id).await {
            warn!("Lifecycle: Failed to remove old container (may not exist): {}", e);
        }

        // 4. Release old port allocations so they can be re-allocated
        // This is critical - ports are tracked by UUID, we need to release them first
        {
            let mut net_mgr = self.network.write().await;
            for alloc in &tracker.allocated_ports {
                if let Ok(port) = alloc.host_port.parse::<u16>() {
                    net_mgr.release_port(port);
                    info!("Lifecycle: Released port {} for recreation", port);
                }
            }
        }

        // 5. Merge update with existing config
        let (new_env, new_limits, _new_ports, new_image, _new_startup) = if let Some(upd) = update {
            (
                upd.env.or(tracker.env.clone()),
                upd.limits.map(|l| l).or(Some(tracker.limits.clone())),
                upd.ports.or(Some(tracker.ports.clone())),
                upd.image.unwrap_or(tracker.image.clone()),
                upd.startup_command.or(tracker.startup_command.clone()),
            )
        } else {
            (
                tracker.env.clone(),
                Some(tracker.limits.clone()),
                Some(tracker.ports.clone()),
                tracker.image.clone(),
                tracker.startup_command.clone(),
            )
        };

        // 6. Build port config from tracker's allocated_ports (preserve existing allocations)
        let ports_map: HashMap<String, String> = tracker.allocated_ports
            .iter()
            .map(|p| (p.container_port.clone(), p.host_port.clone()))
            .collect();

        // 7. Create new container with same UUID
        info!("Lifecycle: Creating new container for UUID {} with {} ports", uuid, ports_map.len());
        let create_req = CreateContainerRequest {
            image: new_image,
            name: Some(tracker.name.clone()),
            description: tracker.description.clone(),
            startup_command: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
            env: new_env,
            ports: if ports_map.is_empty() { None } else { Some(ports_map) },
            volumes: Some(tracker.attached_volumes.clone()),
            command: None,
            working_dir: None,
            restart_policy: None,
            custom_uuid: Some(uuid.to_string()),
            limits: new_limits,
            install_content: None,
            runtime: tracker.runtime.clone(),
            request_id: None,
        };

        match manager.create_with_networking(create_req, &self.network, uuid, &self.volumes_path).await {
            Ok((new_container_id, allocations)) => {
                info!("Lifecycle: New container created: {}", new_container_id);

                // 8. Update tracker with new container ID
                let mut updated_tracker = tracker.clone();
                updated_tracker.container_id = new_container_id.clone();
                updated_tracker.allocated_ports = allocations;
                if let Err(e) = self.container_tracker.save_container(&updated_tracker).await {
                    error!("Lifecycle: Failed to save tracker: {}", e);
                }

                // 9. Update state manager with new container ID
                let _ = self.state_manager.update_container_id(uuid, &new_container_id).await;

                // 10. Start the new container
                info!("Lifecycle: Starting new container {}", new_container_id);
                if let Err(e) = manager.start(&new_container_id).await {
                    error!("Lifecycle: Failed to start container: {}", e);
                    let _ = self.state_manager.update_container_state(uuid, "failed").await;
                } else {
                    let _ = self.state_manager.update_container_state(uuid, "running").await;
                }

                // 11. Unlock
                self.unlock_container(uuid).await;
                
                // Notify remote panel that recreation succeeded
                if let Some(ref remote) = self.remote {
                    let request_id = self.state_manager.get_container(uuid).and_then(|s| s.operation_id);
                    remote
                        .send_install_status_with_container_id_and_id(
                            uuid,
                            "install_success",
                            Some("Container recreated successfully"),
                            None,
                            request_id.as_deref(),
                        )
                        .await;
                }
                
                info!("Lifecycle: Container {} recreation complete, new ID: {}", uuid, new_container_id);
                Ok(new_container_id)
            }
            Err(e) => {
                error!("Lifecycle: Failed to create container: {}", e);
                let _ = self.state_manager.update_container_state(uuid, "failed").await;
                self.unlock_container(uuid).await;
                
                // Notify remote panel that recreation failed
                if let Some(ref remote) = self.remote {
                    let request_id = self.state_manager.get_container(uuid).and_then(|s| s.operation_id);
                    remote
                        .send_install_status_with_container_id_and_id(
                            uuid,
                            "install_failed",
                            Some(&format!("Container recreation failed: {}", e)),
                            None,
                            request_id.as_deref(),
                        )
                        .await;
                }
                
                Err(format!("Failed to create container: {}", e))
            }
        }
    }

    /// Update container environment variables (triggers recreation)
    pub async fn update_env(
        &self,
        uuid: &str,
        env: HashMap<String, String>,
    ) -> Result<String, String> {
        self.recreate_container(uuid, Some(ContainerUpdateRequest {
            env: Some(env),
            limits: None,
            ports: None,
            image: None,
            startup_command: None,
        })).await
    }

    /// Update container resource limits (triggers recreation)
    pub async fn update_limits(
        &self,
        uuid: &str,
        limits: ResourceLimits,
    ) -> Result<String, String> {
        self.recreate_container(uuid, Some(ContainerUpdateRequest {
            env: None,
            limits: Some(limits),
            ports: None,
            image: None,
            startup_command: None,
        })).await
    }

    /// Update container ports (triggers recreation)
    pub async fn update_ports(
        &self,
        uuid: &str,
        ports: HashMap<String, String>,
    ) -> Result<String, String> {
        // First update the port allocations in tracker
        let tracker = self.container_tracker.get_container(uuid).await
            .map_err(|e| e.to_string())?
            .ok_or("Container not found")?;

        // Release old ports and allocate new ones
        let mut net_mgr = self.network.write().await;
        
        // Release existing ports
        for alloc in &tracker.allocated_ports {
            if let Ok(port) = alloc.host_port.parse::<u16>() {
                net_mgr.release_port(port);
            }
        }

        // Allocate new ports
        let new_allocations = net_mgr.auto_allocate_ports(uuid, &ports).await
            .map_err(|e| format!("Port allocation failed: {}", e))?;
        
        drop(net_mgr);

        // Update tracker with new port allocations
        let new_port_allocs: Vec<PortAllocation> = new_allocations
            .iter()
            .map(|(cp, hp): (&String, &String)| PortAllocation {
                container_port: cp.clone(),
                host_port: hp.clone(),
                host_ip: "0.0.0.0".to_string(),
                protocol: "tcp".to_string(),
            })
            .collect();

        self.container_tracker.update_container_ports(uuid, new_port_allocs).await
            .map_err(|e| e.to_string())?;

        // Now recreate with new ports
        self.recreate_container(uuid, Some(ContainerUpdateRequest {
            env: None,
            limits: None,
            ports: Some(new_allocations),
            image: None,
            startup_command: None,
        })).await
    }

    /// Change container image (triggers recreation)
    pub async fn change_image(
        &self,
        uuid: &str,
        image: String,
    ) -> Result<String, String> {
        self.recreate_container(uuid, Some(ContainerUpdateRequest {
            env: None,
            limits: None,
            ports: None,
            image: Some(image),
            startup_command: None,
        })).await
    }

    /// Helper to unlock container
    async fn unlock_container(&self, uuid: &str) {
        let _ = self.state_manager.unlock_container(uuid).await;
    }

    /// Create a new container with installation
    /// Uses temp container pattern: install in temp container, then create final container
    pub async fn create_with_install(
        &self,
        uuid: &str,
        image: &str,
        name: &str,
        description: Option<String>,
        startup_command: Option<Vec<String>>,
        env: Option<HashMap<String, String>>,
        ports: Option<HashMap<String, String>>,
        limits: Option<ResourceLimits>,
        install_script: Option<String>,
    ) -> Result<String, String> {
        info!("Lifecycle: Creating container {} with install", uuid);

        // 1. Lock the container
        self.state_manager.lock_container(uuid, "Installing").await
            .map_err(|e| format!("Failed to lock: {}", e))?;
        self.state_manager.update_container_state(uuid, "installing").await
            .map_err(|e| format!("Failed to update state: {}", e))?;

        let manager = ContainerManager::new(self.docker.as_ref().clone());

        // 2. Create volume directory for this UUID
        let volume_path = format!("{}/{}", self.volumes_path, uuid);
        tokio::fs::create_dir_all(&volume_path).await
            .map_err(|e| format!("Failed to create volume dir: {}", e))?;

        // 3. Create data directory for entrypoint
        let data_path = format!("{}/{}_data", self.volumes_path, uuid);
        tokio::fs::create_dir_all(&data_path).await
            .map_err(|e| format!("Failed to create data dir: {}", e))?;

        // Track install result - None means no install needed, Some(true) = success, Some(false) = failed
        let mut install_result: Option<bool> = None;

        // 4. If install_script provided, run install in temp container
        if let Some(install_script) = &install_script {
            info!("Lifecycle: Running install script in temp container for {}", uuid);

            // Write install script
            let install_script_path = format!("{}/install.sh", data_path);
            tokio::fs::write(&install_script_path, install_script).await
                .map_err(|e| format!("Failed to write install script: {}", e))?;
            
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&install_script_path, std::fs::Permissions::from_mode(0o755));
            }

            // Create temp install container (no ports, no tracking)
            let temp_container_name = format!("{}-install-{}", uuid, chrono::Utc::now().timestamp());
            let abs_volume_path = std::fs::canonicalize(&volume_path)
                .map_err(|e| format!("Failed to get abs path: {}", e))?
                .to_string_lossy()
                .to_string();
            let abs_data_path = std::fs::canonicalize(&data_path)
                .map_err(|e| format!("Failed to get abs data path: {}", e))?
                .to_string_lossy()
                .to_string();

            let temp_config = bollard::container::Config {
                image: Some(image.to_string()),
                cmd: Some(vec!["/bin/sh".to_string(), "/data/install.sh".to_string()]),
                working_dir: Some("/home/container".to_string()),
                tty: Some(true),
                env: Some(vec!["MODE=INSTALL".to_string()]),
                host_config: Some(bollard::models::HostConfig {
                    binds: Some(vec![
                        format!("{}:/home/container", abs_volume_path),
                        format!("{}:/data:ro", abs_data_path),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            };

            let temp_options = bollard::container::CreateContainerOptions {
                name: &temp_container_name,
                platform: None,
            };

            // Pull image first
            manager.pull_image_if_needed(image).await
                .map_err(|e| format!("Failed to pull image: {}", e))?;

            // Create temp container
            let temp_response = self.docker.create_container(Some(temp_options), temp_config).await
                .map_err(|e| format!("Failed to create temp container: {}", e))?;
            let temp_id = temp_response.id;

            info!("Lifecycle: Starting temp install container {}", temp_id);

            // Start temp container
            self.docker.start_container::<String>(&temp_id, None).await
                .map_err(|e| format!("Failed to start temp container: {}", e))?;

            // Wait for install to complete (max 10 minutes)
            let max_wait = 600;
            let mut waited = 0;
            let mut install_success = false;
            let mut install_timed_out = false;

            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                waited += 2;

                if waited > max_wait {
                    error!("Lifecycle: Install timed out for {}", uuid);
                    install_timed_out = true;
                    // Cleanup temp container but continue to create final container
                    let _ = self.docker.remove_container(&temp_id, Some(bollard::container::RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    })).await;
                    break;
                }

                match self.docker.inspect_container(&temp_id, None).await {
                    Ok(info) => {
                        if let Some(state) = info.state {
                            if state.running != Some(true) {
                                let exit_code = state.exit_code.unwrap_or(-1);
                                info!("Lifecycle: Install finished with exit code {}", exit_code);
                                install_success = exit_code == 0;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Lifecycle: Failed to inspect temp container: {}", e);
                        break;
                    }
                }
            }

            // Remove temp container (if not already removed due to timeout)
            if !install_timed_out {
                info!("Lifecycle: Removing temp install container {}", temp_id);
                let _ = self.docker.remove_container(&temp_id, Some(bollard::container::RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                })).await;
            }

            // Track the install result
            if install_timed_out {
                install_result = Some(false);
                warn!("Lifecycle: Install timed out for {}, but continuing to create final container", uuid);
            } else if !install_success {
                install_result = Some(false);
                warn!("Lifecycle: Install script failed for {}, but continuing to create final container", uuid);
            } else {
                install_result = Some(true);
                info!("Lifecycle: Install succeeded for {}", uuid);
            }
        }

        // 5. Write startup entrypoint
        let startup_cmd = startup_command.as_ref()
            .map(|cmd| cmd.join(" "))
            .unwrap_or_else(|| "sleep infinity".to_string());
        let entrypoint_path = format!("{}/entrypoint.sh", data_path);
        tokio::fs::write(&entrypoint_path, &startup_cmd).await
            .map_err(|e| format!("Failed to write entrypoint: {}", e))?;
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&entrypoint_path, std::fs::Permissions::from_mode(0o755));
        }

        // 6. Create final container with ports
        info!("Lifecycle: Creating final container for {}", uuid);
        let limits_for_tracker = limits.clone();
        let create_req = CreateContainerRequest {
            image: image.to_string(),
            name: Some(name.to_string()),
            description,
            startup_command: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
            env,
            ports,
            volumes: None, // Volume is auto-mounted by create_with_networking
            command: None,
            working_dir: None,
            restart_policy: None,
            custom_uuid: Some(uuid.to_string()),
            limits,
            install_content: None,
            runtime: None,
            request_id: None,
        };

        match manager.create_with_networking(create_req, &self.network, uuid, &self.volumes_path).await {
            Ok((container_id, allocations)) => {
                info!("Lifecycle: Final container created: {}", container_id);

                // 7. Determine final state based on install result
                let final_state = match install_result {
                    Some(false) => "install_failed", // Install ran but failed
                    _ => "stopped", // No install or install succeeded - container is stopped, ready to start
                };

                // 8. Update state manager with final state (don't auto-start if install failed)
                self.state_manager.update_container_state(uuid, final_state).await.ok();
                self.state_manager.update_container_id(uuid, &container_id).await.ok();

                // 9. Save tracker
                let tracker = crate::models::ContainerTracker {
                    custom_uuid: uuid.to_string(),
                    container_id: container_id.clone(),
                    name: name.to_string(),
                    image: image.to_string(),
                    description: None,
                    startup_command: startup_command.clone(),
                    created_at: chrono::Utc::now(),
                    limits: limits_for_tracker.unwrap_or_default(),
                    allocated_ports: allocations,
                    attached_volumes: vec![],
                    ports: HashMap::new(),
                    env: None,
                    status: final_state.to_string(),
                    runtime: None,
                };
                self.container_tracker.save_container(&tracker).await.ok();

                self.unlock_container(uuid).await;

                // Notify remote panel that install completed (or failed) so it unlocks
                if let Some(ref remote) = self.remote {
                    let request_id = self.state_manager.get_container(uuid).and_then(|s| s.operation_id);
                    let status = if install_result == Some(false) { "install_failed" } else { "install_success" };
                    let message = if install_result == Some(false) {
                        Some("Install failed")
                    } else {
                        Some("Install complete")
                    };
                    remote
                        .send_install_status_with_container_id_and_id(
                            uuid,
                            status,
                            message,
                            None,
                            request_id.as_deref(),
                        )
                        .await;
                }
                
                if install_result == Some(false) {
                    info!("Lifecycle: Container {} created but install failed - container is available for retry", uuid);
                } else {
                    info!("Lifecycle: Container {} creation complete", uuid);
                }
                
                Ok(container_id)
            }
            Err(e) => {
                error!("Lifecycle: Failed to create final container: {}", e);
                self.state_manager.update_container_state(uuid, "failed").await.ok();
                self.unlock_container(uuid).await;

                if let Some(ref remote) = self.remote {
                    let request_id = self.state_manager.get_container(uuid).and_then(|s| s.operation_id);
                    remote
                        .send_install_status_with_container_id_and_id(
                            uuid,
                            "install_failed",
                            Some(&format!("Failed to create container: {}", e)),
                            None,
                            request_id.as_deref(),
                        )
                        .await;
                }
                Err(format!("Failed to create container: {}", e))
            }
        }
    }

    /// Reinstall a container - simple, stateless approach
    /// 
    /// 1. Write install script to entrypoint.sh
    /// 2. Recreate container with new image
    /// 3. Start container (runs install script)
    /// 4. Wait for install to complete
    /// 5. Write startup command to entrypoint.sh
    /// 6. Start container again (runs startup command)
    /// 7. Callback to panel with result
    /// 
    /// Panel is source of truth - Lightd just executes and reports back.
    pub async fn reinstall(
        &self,
        uuid: &str,
        install_script: &str,
        image: Option<&str>,
        startup_command: Option<&Vec<String>>,
    ) -> Result<String, String> {
        info!("Lifecycle: Starting reinstall for UUID {}", uuid);

        // Load tracker for container info (ports, limits, etc.)
        let tracker = self.container_tracker.get_container(uuid).await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "Container not found".to_string())?;

        let old_container_id = tracker.container_id.clone();
        let manager = ContainerManager::new(self.docker.as_ref().clone());

        // Use provided image/startup or fall back to tracker
        let container_image = image.unwrap_or(&tracker.image);
        
        // Build startup command - if it's ["sh", "-c", "actual command"], extract the actual command
        let startup_cmd = startup_command
            .map(|cmd| {
                // If command is ["sh", "-c", "actual command"], extract just the actual command
                if cmd.len() >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh") && cmd[1] == "-c" {
                    cmd[2..].join(" ")
                } else {
                    cmd.join(" ")
                }
            })
            .or_else(|| tracker.startup_command.as_ref().map(|cmd| {
                if cmd.len() >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh") && cmd[1] == "-c" {
                    cmd[2..].join(" ")
                } else {
                    cmd.join(" ")
                }
            }))
            .unwrap_or_else(|| "sleep infinity".to_string());

        // Setup paths
        let data_path = format!("{}/{}_data", self.volumes_path, uuid);
        let entrypoint_path = format!("{}/entrypoint.sh", data_path);
        
        tokio::fs::create_dir_all(&data_path).await
            .map_err(|e| format!("Failed to create data dir: {}", e))?;

        // Step 1: Write install script to entrypoint.sh
        tokio::fs::write(&entrypoint_path, install_script).await
            .map_err(|e| format!("Failed to write install script: {}", e))?;
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&entrypoint_path, std::fs::Permissions::from_mode(0o755));
        }

        // Step 2: Stop and remove old container
        let _ = manager.stop(&old_container_id).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        let _ = manager.remove(&old_container_id).await;

        // Release old ports
        {
            let mut net_mgr = self.network.write().await;
            for alloc in &tracker.allocated_ports {
                if let Ok(port) = alloc.host_port.parse::<u16>() {
                    net_mgr.release_port(port);
                }
            }
        }

        // Step 3: Pull image and create new container
        manager.pull_image_if_needed(container_image).await
            .map_err(|e| format!("Failed to pull image: {}", e))?;

        let ports_map: HashMap<String, String> = tracker.allocated_ports
            .iter()
            .map(|p| (p.container_port.clone(), p.host_port.clone()))
            .collect();

        let create_req = crate::models::CreateContainerRequest {
            image: container_image.to_string(),
            name: Some(tracker.name.clone()),
            description: tracker.description.clone(),
            startup_command: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
            env: tracker.env.clone(),
            ports: if ports_map.is_empty() { None } else { Some(ports_map) },
            volumes: None,
            command: None,
            working_dir: None,
            restart_policy: None,
            custom_uuid: Some(uuid.to_string()),
            limits: Some(tracker.limits.clone()),
            install_content: None,
            runtime: tracker.runtime.clone(),
            request_id: None,
        };

        let (new_container_id, allocations) = manager.create_with_networking(
            create_req, 
            &self.network, 
            uuid, 
            &self.volumes_path
        ).await.map_err(|e| format!("Failed to create container: {}", e))?;

        info!("Lifecycle: New container created: {}", new_container_id);

        // Update tracker with new container ID, image, and startup command
        let mut updated_tracker = tracker.clone();
        updated_tracker.container_id = new_container_id.clone();
        updated_tracker.allocated_ports = allocations;
        updated_tracker.image = container_image.to_string();
        // Store the actual startup command (not the entrypoint wrapper)
        if let Some(cmd) = startup_command {
            // Store the original command format for future reinstalls
            updated_tracker.startup_command = Some(cmd.clone());
        }
        self.container_tracker.save_container(&updated_tracker).await.ok();

        // Update state manager so websocket can find the container
        self.state_manager.update_container_id(uuid, &new_container_id).await.ok();
        self.state_manager.update_container_state(uuid, "installing").await.ok();

        // Step 4: Start container (runs install script)
        manager.start(&new_container_id).await
            .map_err(|e| format!("Failed to start container: {}", e))?;

        // Step 5: Wait for install to complete (max 10 min)
        let install_success = self.wait_for_container_exit(&new_container_id, 600).await;

        if !install_success {
            self.state_manager.update_container_state(uuid, "install_failed").await.ok();
            return Err("Install script failed or timed out".to_string());
        }

        // Step 6: Write startup command to entrypoint.sh
        info!("Lifecycle: Install succeeded, writing startup command");
        tokio::fs::write(&entrypoint_path, &startup_cmd).await
            .map_err(|e| format!("Failed to write startup command: {}", e))?;

        // Step 7: Start container again (runs startup command)
        if let Err(e) = manager.start(&new_container_id).await {
            warn!("Lifecycle: Failed to start with startup command: {}", e);
            self.state_manager.update_container_state(uuid, "stopped").await.ok();
        } else {
            self.state_manager.update_container_state(uuid, "running").await.ok();
        }

        info!("Lifecycle: Reinstall complete for {}", uuid);
        Ok(new_container_id)
    }

    /// Wait for container to exit (install script completion)
    /// Returns true if exit code is 0, false otherwise or on timeout
    async fn wait_for_container_exit(&self, container_id: &str, timeout_secs: u64) -> bool {
        let mut waited = 0u64;
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            waited += 2;

            if waited > timeout_secs {
                error!("Lifecycle: Container {} timed out after {}s", container_id, timeout_secs);
                return false;
            }

            match self.docker.inspect_container(container_id, None).await {
                Ok(info) => {
                    if let Some(state) = info.state {
                        if state.running != Some(true) {
                            let exit_code = state.exit_code.unwrap_or(-1);
                            info!("Lifecycle: Container exited with code {}", exit_code);
                            return exit_code == 0;
                        }
                    }
                }
                Err(e) => {
                    error!("Lifecycle: Failed to inspect container: {}", e);
                    return false;
                }
            }
        }
    }

    /// Swap the /home/container mount to a different volume path
    /// This recreates the container with the new volume attached
    pub async fn swap_mount(
        &self,
        uuid: &str,
        new_volume_path: &str,
    ) -> Result<String, String> {
        info!("Lifecycle: Swapping mount for {} to {}", uuid, new_volume_path);

        // 1. Lock the container
        self.state_manager.lock_container(uuid, "Swapping mount").await
            .map_err(|e| format!("Failed to lock container: {}", e))?;
        self.state_manager.update_container_state(uuid, "recreating").await
            .map_err(|e| format!("Failed to update state: {}", e))?;

        // 2. Load current tracker data
        let tracker = match self.container_tracker.get_container(uuid).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                self.unlock_container(uuid).await;
                return Err("Container tracker data not found".to_string());
            }
            Err(e) => {
                self.unlock_container(uuid).await;
                return Err(format!("Failed to load container data: {}", e));
            }
        };

        let old_container_id = tracker.container_id.clone();
        let manager = ContainerManager::new(self.docker.as_ref().clone());

        // 3. Stop and remove old container
        info!("Lifecycle: Stopping old container {} for mount swap", old_container_id);
        let _ = manager.stop(&old_container_id).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        info!("Lifecycle: Removing old container {}", old_container_id);
        if let Err(e) = manager.remove(&old_container_id).await {
            warn!("Lifecycle: Failed to remove old container (may not exist): {}", e);
        }

        // 4. Release old port allocations
        {
            let mut net_mgr = self.network.write().await;
            for alloc in &tracker.allocated_ports {
                if let Ok(port) = alloc.host_port.parse::<u16>() {
                    net_mgr.release_port(port);
                    info!("Lifecycle: Released port {} for mount swap", port);
                }
            }
        }

        // 5. Build port config from tracker's allocated_ports
        let ports_map: HashMap<String, String> = tracker.allocated_ports
            .iter()
            .map(|p| (p.container_port.clone(), p.host_port.clone()))
            .collect();

        // 6. Create new container with custom volume path
        info!("Lifecycle: Creating new container for {} with volume {}", uuid, new_volume_path);
        
        // Create container with custom volume binding
        let result = self.create_container_with_custom_volume(
            uuid,
            &tracker,
            new_volume_path,
            ports_map,
        ).await;

        match result {
            Ok(new_container_id) => {
                info!("Lifecycle: Mount swap complete for {}, new ID: {}", uuid, new_container_id);
                
                // Update tracker with custom volume info
                let mut updated_tracker = tracker.clone();
                updated_tracker.container_id = new_container_id.clone();
                
                // Add custom volume to attached_volumes
                let custom_volume = crate::models::VolumeMount {
                    source: new_volume_path.to_string(),
                    target: "/home/container".to_string(),
                    read_only: Some(false),
                };
                updated_tracker.attached_volumes = vec![custom_volume];
                
                self.container_tracker.save_container(&updated_tracker).await.ok();
                self.state_manager.update_container_id(uuid, &new_container_id).await.ok();
                self.state_manager.update_container_state(uuid, "stopped").await.ok();
                
                self.unlock_container(uuid).await;
                Ok(new_container_id)
            }
            Err(e) => {
                error!("Lifecycle: Failed to create container with new mount: {}", e);
                self.state_manager.update_container_state(uuid, "failed").await.ok();
                self.unlock_container(uuid).await;
                Err(e)
            }
        }
    }

    /// Reset mount to default volume (container's own volume)
    pub async fn reset_mount(
        &self,
        uuid: &str,
    ) -> Result<String, String> {
        let default_volume_path = format!("{}/{}", self.volumes_path, uuid);
        info!("Lifecycle: Resetting mount for {} to default {}", uuid, default_volume_path);

        // 1. Lock the container
        self.state_manager.lock_container(uuid, "Resetting mount").await
            .map_err(|e| format!("Failed to lock container: {}", e))?;
        self.state_manager.update_container_state(uuid, "recreating").await
            .map_err(|e| format!("Failed to update state: {}", e))?;

        // 2. Load current tracker data
        let tracker = match self.container_tracker.get_container(uuid).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                self.unlock_container(uuid).await;
                return Err("Container tracker data not found".to_string());
            }
            Err(e) => {
                self.unlock_container(uuid).await;
                return Err(format!("Failed to load container data: {}", e));
            }
        };

        let old_container_id = tracker.container_id.clone();
        let manager = ContainerManager::new(self.docker.as_ref().clone());

        // 3. Stop and remove old container
        info!("Lifecycle: Stopping old container {} for mount reset", old_container_id);
        let _ = manager.stop(&old_container_id).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        info!("Lifecycle: Removing old container {}", old_container_id);
        if let Err(e) = manager.remove(&old_container_id).await {
            warn!("Lifecycle: Failed to remove old container (may not exist): {}", e);
        }

        // 4. Release old port allocations
        {
            let mut net_mgr = self.network.write().await;
            for alloc in &tracker.allocated_ports {
                if let Ok(port) = alloc.host_port.parse::<u16>() {
                    net_mgr.release_port(port);
                }
            }
        }

        // 5. Build port config
        let ports_map: HashMap<String, String> = tracker.allocated_ports
            .iter()
            .map(|p| (p.container_port.clone(), p.host_port.clone()))
            .collect();

        // 6. Recreate using standard method (uses default volume)
        let create_req = CreateContainerRequest {
            image: tracker.image.clone(),
            name: Some(tracker.name.clone()),
            description: tracker.description.clone(),
            startup_command: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
            env: tracker.env.clone(),
            ports: if ports_map.is_empty() { None } else { Some(ports_map) },
            volumes: None, // Use default volume
            command: None,
            working_dir: None,
            restart_policy: None,
            custom_uuid: Some(uuid.to_string()),
            limits: Some(tracker.limits.clone()),
            install_content: None,
            runtime: tracker.runtime.clone(),
            request_id: None,
        };

        match manager.create_with_networking(create_req, &self.network, uuid, &self.volumes_path).await {
            Ok((new_container_id, allocations)) => {
                info!("Lifecycle: Mount reset complete for {}, new ID: {}", uuid, new_container_id);

                // Update tracker - clear custom volumes
                let mut updated_tracker = tracker.clone();
                updated_tracker.container_id = new_container_id.clone();
                updated_tracker.allocated_ports = allocations;
                updated_tracker.attached_volumes = vec![]; // Clear custom volumes
                
                self.container_tracker.save_container(&updated_tracker).await.ok();
                self.state_manager.update_container_id(uuid, &new_container_id).await.ok();
                self.state_manager.update_container_state(uuid, "stopped").await.ok();

                self.unlock_container(uuid).await;
                Ok(new_container_id)
            }
            Err(e) => {
                error!("Lifecycle: Failed to reset mount: {}", e);
                self.state_manager.update_container_state(uuid, "failed").await.ok();
                self.unlock_container(uuid).await;
                Err(format!("Failed to create container: {}", e))
            }
        }
    }

    /// Helper to create container with custom volume path
    async fn create_container_with_custom_volume(
        &self,
        uuid: &str,
        tracker: &crate::models::ContainerTracker,
        volume_path: &str,
        ports_map: HashMap<String, String>,
    ) -> Result<String, String> {
        // Get absolute paths
        let abs_volume_path = std::fs::canonicalize(volume_path)
            .map_err(|e| format!("Failed to get absolute volume path: {}", e))?
            .to_string_lossy()
            .to_string();
        
        let data_path = format!("{}/{}_data", self.volumes_path, uuid);
        let abs_data_path = std::fs::canonicalize(&data_path)
            .map_err(|e| format!("Failed to get absolute data path: {}", e))?
            .to_string_lossy()
            .to_string();

        // Allocate ports
        let allocated_ports = if !ports_map.is_empty() {
            let mut net_mgr = self.network.write().await;
            net_mgr.auto_allocate_ports(uuid, &ports_map).await
                .map_err(|e| format!("Port allocation failed: {}", e))?
        } else {
            HashMap::new()
        };

        // Build port bindings
        let mut port_bindings = std::collections::HashMap::new();
        let mut exposed_ports = std::collections::HashMap::new();

        for (container_port, host_port) in &allocated_ports {
            let port_spec = format!("{}/tcp", container_port);
            exposed_ports.insert(port_spec.clone(), std::collections::HashMap::new());
            
            port_bindings.insert(
                port_spec,
                Some(vec![bollard::models::PortBinding {
                    host_ip: Some("0.0.0.0".to_string()),
                    host_port: Some(host_port.clone()),
                }]),
            );
        }

        // Build environment variables
        let mut env_vars: Vec<String> = tracker.env.as_ref()
            .map(|e| e.iter().map(|(k, v)| format!("{}={}", k, v)).collect())
            .unwrap_or_default();
        
        env_vars.push("LIGHTD_MANAGED=true".to_string());

        // Create container config
        let config = bollard::container::Config {
            image: Some(tracker.image.clone()),
            cmd: Some(vec!["/bin/sh".to_string(), "/data/entrypoint.sh".to_string()]),
            working_dir: Some("/home/container".to_string()),
            tty: Some(true),
            open_stdin: Some(true),
            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            env: Some(env_vars),
            exposed_ports: if exposed_ports.is_empty() { None } else { Some(exposed_ports) },
            host_config: Some(bollard::models::HostConfig {
                binds: Some(vec![
                    format!("{}:/home/container", abs_volume_path),
                    format!("{}:/data:ro", abs_data_path),
                ]),
                port_bindings: if port_bindings.is_empty() { None } else { Some(port_bindings) },
                restart_policy: Some(bollard::models::RestartPolicy {
                    name: Some(bollard::models::RestartPolicyNameEnum::UNLESS_STOPPED),
                    maximum_retry_count: None,
                }),
                oom_kill_disable: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = bollard::container::CreateContainerOptions {
            name: uuid,
            platform: None,
        };

        // Create container
        let response = self.docker.create_container(Some(options), config).await
            .map_err(|e| format!("Failed to create container: {}", e))?;

        // Update port allocations in tracker
        let port_allocs: Vec<PortAllocation> = allocated_ports
            .into_iter()
            .map(|(cp, hp)| PortAllocation {
                container_port: cp,
                host_port: hp,
                host_ip: "0.0.0.0".to_string(),
                protocol: "tcp".to_string(),
            })
            .collect();

        // Save updated tracker
        let mut updated_tracker = tracker.clone();
        updated_tracker.container_id = response.id.clone();
        updated_tracker.allocated_ports = port_allocs;
        self.container_tracker.save_container(&updated_tracker).await.ok();

        Ok(response.id)
    }
}
