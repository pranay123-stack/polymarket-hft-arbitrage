//! Monitoring and metrics module.
//!
//! Provides:
//! - Prometheus metrics export
//! - Performance tracking
//! - Health checks
//! - Alerting

pub mod metrics;
pub mod health;

pub use metrics::MetricsCollector;
pub use health::HealthChecker;
