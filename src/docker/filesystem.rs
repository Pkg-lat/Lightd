//! Deprecated Module
//!
//! This module provides a way to interact with the filesystem of containers.
//! 
//! why?
//! It is deprecated in favor of `filesystem_direct` which uses the volume mounts directly
//! withtout going through the Docker API.(meaning the container should be started for this to work)