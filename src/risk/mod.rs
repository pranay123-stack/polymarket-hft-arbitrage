//! Risk management module.
//!
//! Provides:
//! - Position limits
//! - Daily loss limits
//! - Drawdown protection
//! - Circuit breaker functionality
//! - Order rate limiting
//! - Kelly Criterion position sizing
//! - Stress testing with VaR/CVaR

pub mod manager;
pub mod limits;
pub mod circuit_breaker;
pub mod kelly;
pub mod stress_test;

pub use manager::RiskManager;
pub use limits::RiskLimits;
pub use circuit_breaker::CircuitBreaker;
pub use kelly::{
    KellyCalculator, KellyConfig, KellyInput, KellyResult,
    kelly_fraction, kelly_position_size,
};
pub use stress_test::{
    StressTestEngine, StressTestConfig, StressScenario, StressTestResult,
    VarMethod, VarResult, RiskMetrics, RiskPosition, ConfidenceLevel,
};
