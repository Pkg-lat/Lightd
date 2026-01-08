//! Firewall Module - Per-container iptables rule management
//!
//! Manages firewall rules for containers using iptables.
//! Each container gets its own chain for easy rule management.
//!
//! Chain naming: LIGHTD-{container_uuid_short}
//! Rules are applied based on container IP from Docker network.

pub mod manager;
pub mod rules;

pub use manager::FirewallManager;
pub use rules::{FirewallRule, FirewallAction, FirewallDirection, FirewallProtocol, ContainerFirewall};
