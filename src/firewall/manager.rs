//! Firewall Manager - iptables rule management for containers
//!
//! Creates per-container chains and manages rules using iptables.
//! Requires root/sudo access to modify iptables.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};

use super::rules::{ContainerFirewall, FirewallRule, FirewallDirection, FirewallAction, FirewallProtocol};

/// Chain prefix for lightd-managed chains
const CHAIN_PREFIX: &str = "LIGHTD";

/// Firewall manager for container iptables rules
pub struct FirewallManager {
    /// Container UUID -> Container IP mapping
    container_ips: Arc<RwLock<HashMap<String, String>>>,
    /// Container UUID -> Firewall config
    configs: Arc<RwLock<HashMap<String, ContainerFirewall>>>,
    /// Whether iptables is available
    iptables_available: bool,
}

impl FirewallManager {
    /// Create a new firewall manager
    pub async fn new() -> Self {
        let iptables_available = Self::check_iptables().await;
        
        if iptables_available {
            info!("Firewall manager initialized - iptables available");
        } else {
            warn!("Firewall manager initialized - iptables NOT available (rules will not be enforced)");
        }
        
        Self {
            container_ips: Arc::new(RwLock::new(HashMap::new())),
            configs: Arc::new(RwLock::new(HashMap::new())),
            iptables_available,
        }
    }

    /// Check if iptables is available
    async fn check_iptables() -> bool {
        match Command::new("iptables").arg("--version").output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Get chain name for a container
    fn get_chain_name(container_uuid: &str) -> String {
        // Use first 12 chars of UUID for chain name (iptables has length limits)
        let short_uuid = &container_uuid[..12.min(container_uuid.len())];
        format!("{}-{}", CHAIN_PREFIX, short_uuid.to_uppercase())
    }

    /// Register a container with its IP
    pub async fn register_container(&self, container_uuid: &str, container_ip: &str) {
        let mut ips = self.container_ips.write().await;
        ips.insert(container_uuid.to_string(), container_ip.to_string());
        info!("Registered container {} with IP {}", container_uuid, container_ip);
    }

    /// Unregister a container
    pub async fn unregister_container(&self, container_uuid: &str) {
        let mut ips = self.container_ips.write().await;
        ips.remove(container_uuid);
        
        let mut configs = self.configs.write().await;
        configs.remove(container_uuid);
        
        info!("Unregistered container {}", container_uuid);
    }

    /// Get container IP
    pub async fn get_container_ip(&self, container_uuid: &str) -> Option<String> {
        let ips = self.container_ips.read().await;
        ips.get(container_uuid).cloned()
    }

    /// Set firewall config for a container
    pub async fn set_config(&self, container_uuid: &str, config: ContainerFirewall) {
        let mut configs = self.configs.write().await;
        configs.insert(container_uuid.to_string(), config);
    }

    /// Get firewall config for a container
    pub async fn get_config(&self, container_uuid: &str) -> Option<ContainerFirewall> {
        let configs = self.configs.read().await;
        configs.get(container_uuid).cloned()
    }


    /// Initialize firewall chains for a container
    /// Creates the container-specific chain and hooks it into FORWARD
    pub async fn init_container_firewall(&self, container_uuid: &str) -> Result<(), String> {
        if !self.iptables_available {
            debug!("iptables not available, skipping firewall init for {}", container_uuid);
            return Ok(());
        }

        let chain_name = Self::get_chain_name(container_uuid);
        let container_ip = self.get_container_ip(container_uuid).await
            .ok_or_else(|| format!("Container {} IP not registered", container_uuid))?;

        info!("Initializing firewall chain {} for container {} (IP: {})", 
              chain_name, container_uuid, container_ip);

        // Create the chain (ignore error if exists)
        let _ = self.run_iptables(&["-N", &chain_name]).await;

        // Flush any existing rules in the chain
        self.run_iptables(&["-F", &chain_name]).await?;

        // Hook into FORWARD chain for traffic to/from container
        // First remove any existing hooks (ignore errors)
        let _ = self.run_iptables(&["-D", "FORWARD", "-s", &container_ip, "-j", &chain_name]).await;
        let _ = self.run_iptables(&["-D", "FORWARD", "-d", &container_ip, "-j", &chain_name]).await;

        // Add hooks at the beginning of FORWARD
        self.run_iptables(&["-I", "FORWARD", "1", "-s", &container_ip, "-j", &chain_name]).await?;
        self.run_iptables(&["-I", "FORWARD", "1", "-d", &container_ip, "-j", &chain_name]).await?;

        info!("Firewall chain {} initialized", chain_name);
        Ok(())
    }

    /// Remove firewall chains for a container
    pub async fn cleanup_container_firewall(&self, container_uuid: &str) -> Result<(), String> {
        if !self.iptables_available {
            return Ok(());
        }

        let chain_name = Self::get_chain_name(container_uuid);
        
        // Get container IP (might not exist if already unregistered)
        if let Some(container_ip) = self.get_container_ip(container_uuid).await {
            // Remove hooks from FORWARD
            let _ = self.run_iptables(&["-D", "FORWARD", "-s", &container_ip, "-j", &chain_name]).await;
            let _ = self.run_iptables(&["-D", "FORWARD", "-d", &container_ip, "-j", &chain_name]).await;
        }

        // Flush and delete the chain
        let _ = self.run_iptables(&["-F", &chain_name]).await;
        let _ = self.run_iptables(&["-X", &chain_name]).await;

        info!("Firewall chain {} cleaned up", chain_name);
        Ok(())
    }

    /// Apply firewall rules for a container
    pub async fn apply_rules(&self, container_uuid: &str) -> Result<(), String> {
        if !self.iptables_available {
            debug!("iptables not available, skipping rule application for {}", container_uuid);
            return Ok(());
        }

        let config = self.get_config(container_uuid).await
            .unwrap_or_else(ContainerFirewall::permissive);

        if !config.enabled {
            debug!("Firewall disabled for container {}", container_uuid);
            return Ok(());
        }

        let chain_name = Self::get_chain_name(container_uuid);
        let container_ip = self.get_container_ip(container_uuid).await
            .ok_or_else(|| format!("Container {} IP not registered", container_uuid))?;

        info!("Applying {} firewall rules for container {}", config.rules.len(), container_uuid);

        // Flush existing rules
        self.run_iptables(&["-F", &chain_name]).await?;

        // Apply rules in priority order
        for rule in config.get_sorted_rules() {
            self.apply_rule(&chain_name, &container_ip, rule).await?;
        }

        // Apply default policies at the end
        // For inbound (traffic TO container)
        let inbound_default = if config.default_inbound_allow { "ACCEPT" } else { "DROP" };
        self.run_iptables(&["-A", &chain_name, "-d", &container_ip, "-j", inbound_default]).await?;

        // For outbound (traffic FROM container)
        let outbound_default = if config.default_outbound_allow { "ACCEPT" } else { "DROP" };
        self.run_iptables(&["-A", &chain_name, "-s", &container_ip, "-j", outbound_default]).await?;

        info!("Firewall rules applied for container {}", container_uuid);
        Ok(())
    }

    /// Apply a single rule
    async fn apply_rule(&self, chain_name: &str, container_ip: &str, rule: &FirewallRule) -> Result<(), String> {
        let mut args = vec!["-A", chain_name];

        // Direction determines source/destination
        match rule.direction {
            FirewallDirection::Inbound => {
                args.push("-d");
                args.push(container_ip);
                if let Some(ref remote) = rule.remote_ip {
                    args.push("-s");
                    args.push(remote);
                }
            }
            FirewallDirection::Outbound => {
                args.push("-s");
                args.push(container_ip);
                if let Some(ref remote) = rule.remote_ip {
                    args.push("-d");
                    args.push(remote);
                }
            }
        }

        // Protocol
        if rule.protocol != FirewallProtocol::All {
            args.push("-p");
            let proto_str = match rule.protocol {
                FirewallProtocol::Tcp => "tcp",
                FirewallProtocol::Udp => "udp",
                FirewallProtocol::Icmp => "icmp",
                FirewallProtocol::All => "all",
            };
            args.push(proto_str);

            // Port (only for tcp/udp)
            if let Some(ref port) = rule.port {
                if rule.protocol == FirewallProtocol::Tcp || rule.protocol == FirewallProtocol::Udp {
                    args.push("--dport");
                    args.push(port);
                }
            }
        }

        // Action
        args.push("-j");
        let action_str = match rule.action {
            FirewallAction::Allow => "ACCEPT",
            FirewallAction::Drop => "DROP",
            FirewallAction::Reject => "REJECT",
            FirewallAction::Log => "LOG",
        };
        args.push(action_str);

        // Add comment if description exists
        if let Some(ref desc) = rule.description {
            args.push("-m");
            args.push("comment");
            args.push("--comment");
            args.push(desc);
        }

        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let args_refs: Vec<&str> = args_owned.iter().map(|s| s.as_str()).collect();
        
        self.run_iptables(&args_refs).await
    }

    /// Run an iptables command
    async fn run_iptables(&self, args: &[&str]) -> Result<(), String> {
        debug!("Running: iptables {}", args.join(" "));
        
        let output = Command::new("iptables")
            .args(args)
            .output()
            .await
            .map_err(|e| format!("Failed to run iptables: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Some errors are expected (e.g., chain already exists)
            if !stderr.contains("Chain already exists") && 
               !stderr.contains("No chain/target/match") &&
               !stderr.contains("Bad rule") {
                error!("iptables error: {}", stderr);
                return Err(format!("iptables failed: {}", stderr));
            }
        }

        Ok(())
    }


    /// Add a rule to a container's firewall
    pub async fn add_rule(&self, container_uuid: &str, rule: FirewallRule) -> Result<String, String> {
        let mut configs = self.configs.write().await;
        let config = configs.entry(container_uuid.to_string())
            .or_insert_with(ContainerFirewall::permissive);
        
        let rule_id = rule.id.clone();
        config.add_rule(rule);
        drop(configs);

        // Re-apply all rules
        self.apply_rules(container_uuid).await?;
        
        Ok(rule_id)
    }

    /// Remove a rule from a container's firewall
    pub async fn remove_rule(&self, container_uuid: &str, rule_id: &str) -> Result<bool, String> {
        let mut configs = self.configs.write().await;
        
        if let Some(config) = configs.get_mut(container_uuid) {
            let removed = config.remove_rule(rule_id);
            drop(configs);
            
            if removed {
                // Re-apply all rules
                self.apply_rules(container_uuid).await?;
            }
            
            Ok(removed)
        } else {
            Ok(false)
        }
    }

    /// Enable/disable firewall for a container
    pub async fn set_enabled(&self, container_uuid: &str, enabled: bool) -> Result<(), String> {
        let mut configs = self.configs.write().await;
        let config = configs.entry(container_uuid.to_string())
            .or_insert_with(ContainerFirewall::permissive);
        
        config.enabled = enabled;
        drop(configs);

        if enabled {
            self.apply_rules(container_uuid).await?;
        } else {
            // Flush rules but keep chain
            if self.iptables_available {
                let chain_name = Self::get_chain_name(container_uuid);
                let _ = self.run_iptables(&["-F", &chain_name]).await;
            }
        }

        Ok(())
    }

    /// Set default policies for a container
    pub async fn set_default_policies(
        &self, 
        container_uuid: &str, 
        inbound_allow: bool, 
        outbound_allow: bool
    ) -> Result<(), String> {
        let mut configs = self.configs.write().await;
        let config = configs.entry(container_uuid.to_string())
            .or_insert_with(ContainerFirewall::permissive);
        
        config.default_inbound_allow = inbound_allow;
        config.default_outbound_allow = outbound_allow;
        drop(configs);

        self.apply_rules(container_uuid).await
    }

    /// Get all rules for a container
    pub async fn get_rules(&self, container_uuid: &str) -> Vec<FirewallRule> {
        let configs = self.configs.read().await;
        configs.get(container_uuid)
            .map(|c| c.rules.clone())
            .unwrap_or_default()
    }

    /// List all container chains managed by lightd
    pub async fn list_managed_chains(&self) -> Result<Vec<String>, String> {
        if !self.iptables_available {
            return Ok(vec![]);
        }

        let output = Command::new("iptables")
            .args(&["-L", "-n"])
            .output()
            .await
            .map_err(|e| format!("Failed to list chains: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let chains: Vec<String> = stdout
            .lines()
            .filter(|line| line.starts_with("Chain LIGHTD-"))
            .filter_map(|line| {
                line.split_whitespace()
                    .nth(1)
                    .map(|s| s.to_string())
            })
            .collect();

        Ok(chains)
    }

    /// Cleanup all lightd-managed chains (for daemon shutdown)
    pub async fn cleanup_all(&self) -> Result<(), String> {
        if !self.iptables_available {
            return Ok(());
        }

        info!("Cleaning up all lightd firewall chains");

        let chains = self.list_managed_chains().await?;
        
        for chain in chains {
            // Remove references from FORWARD
            let _ = self.run_iptables(&["-D", "FORWARD", "-j", &chain]).await;
            // Flush and delete
            let _ = self.run_iptables(&["-F", &chain]).await;
            let _ = self.run_iptables(&["-X", &chain]).await;
        }

        info!("All lightd firewall chains cleaned up");
        Ok(())
    }

    /// Check if iptables is available
    pub fn is_available(&self) -> bool {
        self.iptables_available
    }
}

/// Common firewall presets
impl FirewallManager {
    /// Block all outbound except DNS and HTTP/HTTPS
    pub fn preset_web_only() -> ContainerFirewall {
        let mut config = ContainerFirewall::restrictive();
        
        // Allow DNS
        config.add_rule(FirewallRule::allow(FirewallDirection::Outbound, FirewallProtocol::Udp)
            .with_port("53")
            .with_description("Allow DNS"));
        config.add_rule(FirewallRule::allow(FirewallDirection::Outbound, FirewallProtocol::Tcp)
            .with_port("53")
            .with_description("Allow DNS TCP"));
        
        // Allow HTTP/HTTPS
        config.add_rule(FirewallRule::allow(FirewallDirection::Outbound, FirewallProtocol::Tcp)
            .with_port("80")
            .with_description("Allow HTTP"));
        config.add_rule(FirewallRule::allow(FirewallDirection::Outbound, FirewallProtocol::Tcp)
            .with_port("443")
            .with_description("Allow HTTPS"));
        
        // Allow all inbound (for server applications)
        config.default_inbound_allow = true;
        
        config
    }

    /// Block specific IPs (blacklist mode)
    pub fn preset_blacklist(blocked_ips: Vec<String>) -> ContainerFirewall {
        let mut config = ContainerFirewall::permissive();
        
        for (i, ip) in blocked_ips.into_iter().enumerate() {
            config.add_rule(FirewallRule::drop(FirewallDirection::Outbound, FirewallProtocol::All)
                .with_remote_ip(&ip)
                .with_priority(i as i32)
                .with_description(&format!("Block {}", ip)));
        }
        
        config
    }

    /// Allow only specific IPs (whitelist mode)
    pub fn preset_whitelist(allowed_ips: Vec<String>) -> ContainerFirewall {
        let mut config = ContainerFirewall::restrictive();
        
        for (i, ip) in allowed_ips.into_iter().enumerate() {
            config.add_rule(FirewallRule::allow(FirewallDirection::Outbound, FirewallProtocol::All)
                .with_remote_ip(&ip)
                .with_priority(i as i32)
                .with_description(&format!("Allow {}", ip)));
        }
        
        // Allow all inbound
        config.default_inbound_allow = true;
        
        config
    }
}
