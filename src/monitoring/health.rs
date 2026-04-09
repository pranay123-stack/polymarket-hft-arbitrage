//! Health check functionality.

use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Health check status
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub components: Vec<ComponentHealth>,
    pub uptime_secs: u64,
    pub version: &'static str,
}

/// Individual component health
#[derive(Debug, Clone, Serialize)]
pub struct ComponentHealth {
    pub name: String,
    pub healthy: bool,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

/// Health checker
pub struct HealthChecker {
    start_time: Instant,
    components: Arc<RwLock<Vec<ComponentHealth>>>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            components: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a component
    pub async fn register_component(&self, name: &str) {
        let mut components = self.components.write().await;
        components.push(ComponentHealth {
            name: name.to_string(),
            healthy: false,
            latency_ms: None,
            message: Some("Not yet checked".to_string()),
        });
    }

    /// Update component health
    pub async fn update_component(
        &self,
        name: &str,
        healthy: bool,
        latency_ms: Option<u64>,
        message: Option<String>,
    ) {
        let mut components = self.components.write().await;
        if let Some(component) = components.iter_mut().find(|c| c.name == name) {
            component.healthy = healthy;
            component.latency_ms = latency_ms;
            component.message = message;
        }
    }

    /// Get overall health status
    pub async fn get_status(&self) -> HealthStatus {
        let components = self.components.read().await;

        let healthy = components.iter().all(|c| c.healthy);
        let uptime = self.start_time.elapsed().as_secs();

        HealthStatus {
            healthy,
            components: components.clone(),
            uptime_secs: uptime,
            version: env!("CARGO_PKG_VERSION"),
        }
    }

    /// Quick health check (returns true/false)
    pub async fn is_healthy(&self) -> bool {
        let components = self.components.read().await;
        components.iter().all(|c| c.healthy)
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HealthChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthChecker")
            .field("uptime_secs", &self.start_time.elapsed().as_secs())
            .finish()
    }
}
