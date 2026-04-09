//! Performance analytics module for cross-platform arbitrage.
//!
//! Provides:
//! - Real-time P&L tracking
//! - Strategy performance metrics
//! - Venue comparison analytics
//! - Risk-adjusted returns
//! - Dashboard data endpoints

pub mod performance_tracker;
pub mod strategy_analytics;
pub mod venue_analytics;
pub mod dashboard;

pub use performance_tracker::{PnLTracker, PnLSnapshot, PnLSeries};
pub use strategy_analytics::{StrategyAnalytics, StrategyMetrics, StrategyComparison};
pub use venue_analytics::{VenueAnalytics, VenueComparison, VenueHealth};
pub use dashboard::{DashboardData, DashboardEndpoint, TimeRange};
