//! Risk management module.
//!
//! Provides:
//! - Position limits
//! - Daily loss limits
//! - Drawdown protection
//! - Circuit breaker functionality
//! - Order rate limiting

pub mod manager;
pub mod limits;
pub mod circuit_breaker;

pub use manager::RiskManager;
pub use limits::RiskLimits;
pub use circuit_breaker::CircuitBreaker;
