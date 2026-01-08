//! Firewall Rule Types
//!
//! Defines the structure of firewall rules that can be applied to containers.

use serde::{Deserialize, Serialize};

/// Direction of traffic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FirewallDirection {
    /// Incoming traffic to the container
    Inbound,
    /// Outgoing traffic from the container
    Outbound,
}

impl std::fmt::Display for FirewallDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FirewallDirection::Inbound => write!(f, "inbound"),
            FirewallDirection::Outbound => write!(f, "outbound"),
        }
    }
}

/// Action to take on matching traffic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FirewallAction {
    /// Allow the traffic
    Allow,
    /// Drop the traffic silently
    Drop,
    /// Reject with ICMP response
    Reject,
    /// Log the traffic (and continue processing)
    Log,
}

impl std::fmt::Display for FirewallAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FirewallAction::Allow => write!(f, "ACCEPT"),
            FirewallAction::Drop => write!(f, "DROP"),
            FirewallAction::Reject => write!(f, "REJECT"),
            FirewallAction::Log => write!(f, "LOG"),
        }
    }
}

/// Network protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FirewallProtocol {
    Tcp,
    Udp,
    Icmp,
    All,
}

impl std::fmt::Display for FirewallProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FirewallProtocol::Tcp => write!(f, "tcp"),
            FirewallProtocol::Udp => write!(f, "udp"),
            FirewallProtocol::Icmp => write!(f, "icmp"),
            FirewallProtocol::All => write!(f, "all"),
        }
    }
}

/// A single firewall rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirewallRule {
    /// Unique rule ID
    pub id: String,
    /// Rule description
    pub description: Option<String>,
    /// Traffic direction
    pub direction: FirewallDirection,
    /// Action to take
    pub action: FirewallAction,
    /// Protocol (tcp, udp, icmp, all)
    pub protocol: FirewallProtocol,
    /// Source IP/CIDR (for inbound) or destination (for outbound)
    /// None means any
    pub remote_ip: Option<String>,
    /// Port or port range (e.g., "80", "8000-9000")
    /// None means any port
    pub port: Option<String>,
    /// Priority (lower = higher priority, applied first)
    pub priority: i32,
    /// Whether rule is enabled
    pub enabled: bool,
}

impl FirewallRule {
    /// Create a new allow rule
    pub fn allow(direction: FirewallDirection, protocol: FirewallProtocol) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            description: None,
            direction,
            action: FirewallAction::Allow,
            protocol,
            remote_ip: None,
            port: None,
            priority: 100,
            enabled: true,
        }
    }

    /// Create a new drop rule
    pub fn drop(direction: FirewallDirection, protocol: FirewallProtocol) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            description: None,
            direction,
            action: FirewallAction::Drop,
            protocol,
            remote_ip: None,
            port: None,
            priority: 100,
            enabled: true,
        }
    }

    /// Set remote IP/CIDR
    pub fn with_remote_ip(mut self, ip: &str) -> Self {
        self.remote_ip = Some(ip.to_string());
        self
    }

    /// Set port
    pub fn with_port(mut self, port: &str) -> Self {
        self.port = Some(port.to_string());
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }
}

/// Container firewall configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerFirewall {
    /// Whether firewall is enabled for this container
    pub enabled: bool,
    /// Default policy for inbound traffic (true = allow, false = drop)
    pub default_inbound_allow: bool,
    /// Default policy for outbound traffic (true = allow, false = drop)
    pub default_outbound_allow: bool,
    /// List of firewall rules
    pub rules: Vec<FirewallRule>,
}

impl ContainerFirewall {
    /// Create with default permissive policy
    pub fn permissive() -> Self {
        Self {
            enabled: true,
            default_inbound_allow: true,
            default_outbound_allow: true,
            rules: vec![],
        }
    }

    /// Create with default restrictive policy (drop all, must whitelist)
    pub fn restrictive() -> Self {
        Self {
            enabled: true,
            default_inbound_allow: false,
            default_outbound_allow: false,
            rules: vec![],
        }
    }

    /// Add a rule
    pub fn add_rule(&mut self, rule: FirewallRule) {
        self.rules.push(rule);
        self.rules.sort_by_key(|r| r.priority);
    }

    /// Remove a rule by ID
    pub fn remove_rule(&mut self, rule_id: &str) -> bool {
        let len_before = self.rules.len();
        self.rules.retain(|r| r.id != rule_id);
        self.rules.len() < len_before
    }

    /// Get rules sorted by priority
    pub fn get_sorted_rules(&self) -> Vec<&FirewallRule> {
        let mut rules: Vec<_> = self.rules.iter().filter(|r| r.enabled).collect();
        rules.sort_by_key(|r| r.priority);
        rules
    }
}
