//! Firewall Handlers - API endpoints for container firewall management

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    firewall::{FirewallRule, FirewallAction, FirewallDirection, FirewallProtocol, FirewallManager, ContainerFirewall},
    models::ApiResponse,
    types::AppState,
};

#[derive(Debug, Deserialize)]
pub struct CreateRuleRequest {
    pub description: Option<String>,
    pub direction: FirewallDirection,
    pub action: FirewallAction,
    pub protocol: FirewallProtocol,
    pub remote_ip: Option<String>,
    pub port: Option<String>,
    pub priority: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct SetPoliciesRequest {
    pub default_inbound_allow: bool,
    pub default_outbound_allow: bool,
}

#[derive(Debug, Deserialize)]
pub struct SetEnabledRequest {
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApplyPresetRequest {
    pub preset: String,
    pub ips: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct FirewallStatus {
    pub container_uuid: String,
    pub enabled: bool,
    pub default_inbound_allow: bool,
    pub default_outbound_allow: bool,
    pub rule_count: usize,
    pub iptables_available: bool,
}

#[derive(Debug, Serialize)]
pub struct FirewallRulesResponse {
    pub container_uuid: String,
    pub rules: Vec<FirewallRule>,
}

/// Get firewall status for a container
pub async fn get_firewall_status(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<FirewallStatus>>, StatusCode> {
    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    let config: ContainerFirewall = state.firewall.get_config(&container_uuid).await
        .unwrap_or_default();

    Ok(Json(ApiResponse::success(FirewallStatus {
        container_uuid,
        enabled: config.enabled,
        default_inbound_allow: config.default_inbound_allow,
        default_outbound_allow: config.default_outbound_allow,
        rule_count: config.rules.len(),
        iptables_available: state.firewall.is_available(),
    })))
}

/// Get all firewall rules for a container
pub async fn get_firewall_rules(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<FirewallRulesResponse>>, StatusCode> {
    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    let rules = state.firewall.get_rules(&container_uuid).await;

    Ok(Json(ApiResponse::success(FirewallRulesResponse {
        container_uuid,
        rules,
    })))
}

/// Add a firewall rule
pub async fn add_firewall_rule(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
    Json(req): Json<CreateRuleRequest>,
) -> Result<Json<ApiResponse<FirewallRule>>, StatusCode> {
    info!("Adding firewall rule for container {}", container_uuid);

    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    let mut rule = FirewallRule {
        id: uuid::Uuid::new_v4().to_string(),
        description: req.description,
        direction: req.direction,
        action: req.action,
        protocol: req.protocol,
        remote_ip: req.remote_ip,
        port: req.port,
        priority: req.priority.unwrap_or(100),
        enabled: true,
    };

    match state.firewall.add_rule(&container_uuid, rule.clone()).await {
        Ok(rule_id) => {
            rule.id = rule_id;
            Ok(Json(ApiResponse::success(rule)))
        }
        Err(e) => {
            error!("Failed to add firewall rule: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Remove a firewall rule
pub async fn remove_firewall_rule(
    State(state): State<AppState>,
    Path((container_uuid, rule_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Removing firewall rule {} from container {}", rule_id, container_uuid);

    match state.firewall.remove_rule(&container_uuid, &rule_id).await {
        Ok(true) => Ok(Json(ApiResponse::success(format!("Rule {} removed", rule_id)))),
        Ok(false) => Ok(Json(ApiResponse::error(format!("Rule {} not found", rule_id)))),
        Err(e) => {
            error!("Failed to remove firewall rule: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Enable/disable firewall for a container
pub async fn set_firewall_enabled(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
    Json(req): Json<SetEnabledRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Setting firewall enabled={} for container {}", req.enabled, container_uuid);

    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    match state.firewall.set_enabled(&container_uuid, req.enabled).await {
        Ok(_) => {
            let status = if req.enabled { "enabled" } else { "disabled" };
            Ok(Json(ApiResponse::success(format!("Firewall {} for container {}", status, container_uuid))))
        }
        Err(e) => {
            error!("Failed to set firewall enabled: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Set default policies for a container
pub async fn set_firewall_policies(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
    Json(req): Json<SetPoliciesRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Setting firewall policies for container {}: inbound={}, outbound={}", 
          container_uuid, req.default_inbound_allow, req.default_outbound_allow);

    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    match state.firewall.set_default_policies(
        &container_uuid, 
        req.default_inbound_allow, 
        req.default_outbound_allow
    ).await {
        Ok(_) => Ok(Json(ApiResponse::success("Policies updated".to_string()))),
        Err(e) => {
            error!("Failed to set firewall policies: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}


/// Apply a firewall preset
pub async fn apply_firewall_preset(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
    Json(req): Json<ApplyPresetRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Applying firewall preset '{}' for container {}", req.preset, container_uuid);

    // Check if container exists
    if state.state_manager.get_container(&container_uuid).is_none() {
        return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid))));
    }

    let config = match req.preset.as_str() {
        "web_only" => FirewallManager::preset_web_only(),
        "blacklist" => {
            let ips = req.ips.unwrap_or_default();
            FirewallManager::preset_blacklist(ips)
        }
        "whitelist" => {
            let ips = req.ips.unwrap_or_default();
            if ips.is_empty() {
                return Ok(Json(ApiResponse::error("Whitelist preset requires 'ips' array".to_string())));
            }
            FirewallManager::preset_whitelist(ips)
        }
        "permissive" => ContainerFirewall::permissive(),
        "restrictive" => ContainerFirewall::restrictive(),
        _ => {
            return Ok(Json(ApiResponse::error(format!("Unknown preset: {}", req.preset))));
        }
    };

    state.firewall.set_config(&container_uuid, config).await;
    
    match state.firewall.apply_rules(&container_uuid).await {
        Ok(_) => Ok(Json(ApiResponse::success(format!("Preset '{}' applied", req.preset)))),
        Err(e) => {
            error!("Failed to apply firewall preset: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Initialize firewall for a container (called when container starts)
pub async fn init_container_firewall(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Initializing firewall for container {}", container_uuid);

    // Get container IP from Docker
    let container_state = match state.state_manager.get_container(&container_uuid) {
        Some(s) => s,
        None => return Ok(Json(ApiResponse::error(format!("Container {} not found", container_uuid)))),
    };

    let container_id = match &container_state.container_id {
        Some(id) => id.clone(),
        None => return Ok(Json(ApiResponse::error("Container has no Docker ID".to_string()))),
    };

    // Get container IP from Docker inspect
    match state.docker.client.inspect_container(&container_id, None).await {
        Ok(info) => {
            if let Some(network_settings) = info.network_settings {
                if let Some(networks) = network_settings.networks {
                    // Get IP from first network
                    if let Some((_, network)) = networks.iter().next() {
                        if let Some(ip) = &network.ip_address {
                            if !ip.is_empty() {
                                // Register container IP
                                state.firewall.register_container(&container_uuid, ip).await;
                                
                                // Initialize firewall chain
                                if let Err(e) = state.firewall.init_container_firewall(&container_uuid).await {
                                    return Ok(Json(ApiResponse::error(format!("Failed to init firewall: {}", e))));
                                }
                                
                                // Apply any existing rules
                                if let Err(e) = state.firewall.apply_rules(&container_uuid).await {
                                    return Ok(Json(ApiResponse::error(format!("Failed to apply rules: {}", e))));
                                }
                                
                                return Ok(Json(ApiResponse::success(format!("Firewall initialized for {} (IP: {})", container_uuid, ip))));
                            }
                        }
                    }
                }
            }
            Ok(Json(ApiResponse::error("Could not determine container IP".to_string())))
        }
        Err(e) => {
            error!("Failed to inspect container: {}", e);
            Ok(Json(ApiResponse::error(format!("Failed to inspect container: {}", e))))
        }
    }
}

/// Cleanup firewall for a container (called when container stops)
pub async fn cleanup_container_firewall(
    State(state): State<AppState>,
    Path(container_uuid): Path<String>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    info!("Cleaning up firewall for container {}", container_uuid);

    match state.firewall.cleanup_container_firewall(&container_uuid).await {
        Ok(_) => {
            state.firewall.unregister_container(&container_uuid).await;
            Ok(Json(ApiResponse::success(format!("Firewall cleaned up for {}", container_uuid))))
        }
        Err(e) => {
            error!("Failed to cleanup firewall: {}", e);
            Ok(Json(ApiResponse::error(e)))
        }
    }
}

/// Get iptables availability status
pub async fn get_firewall_availability(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    Ok(Json(ApiResponse::success(serde_json::json!({
        "iptables_available": state.firewall.is_available(),
        "message": if state.firewall.is_available() {
            "iptables is available and firewall rules will be enforced"
        } else {
            "iptables is not available - firewall rules will not be enforced"
        }
    }))))
}
