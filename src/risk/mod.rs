//! Risk management module.
//!
//! Provides:
//! - Position limits
//! - Daily loss limits
//! - Drawdown protection
//! - Circuit breaker functionality
//! - Order rate limiting
//! - Kelly Criterion position sizing

pub mod manager;
pub mod limits;
pub mod circuit_breaker;
pub mod kelly;

pub use manager::RiskManager;
pub use limits::RiskLimits;
pub use circuit_breaker::CircuitBreaker;
pub use kelly::{
    KellyCalculator, KellyConfig, KellyInput, KellyResult,
    kelly_fraction, kelly_position_size,
};
