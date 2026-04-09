//! Polymarket Cross-Platform HFT Arbitrage Bot
//!
//! A production-grade, high-frequency trading bot for cross-platform prediction market arbitrage.
//! Supports Polymarket, Kalshi, and Opinion venues with sophisticated execution and risk management.
//!
//! # Features
//!
//! - **Cross-Platform Arbitrage**: Polymarket ↔ Kalshi ↔ Opinion venue arbitrage
//! - **Multi-Strategy Detection**: Binary, multi-outcome, cross-market, temporal, and cross-venue strategies
//! - **Sophisticated Execution**: Leg sequencing, partial fill handling, automatic hedge/unwind
//! - **Professional Risk Management**: Position limits, drawdown protection, circuit breakers
//! - **Real-Time Market Data**: WebSocket feeds from all venues with unified order book aggregation
//! - **Position Reconciliation**: Cross-venue position tracking with drift detection
//! - **Market Discovery**: Automatic mapping of equivalent markets across venues
//! - **Comprehensive Simulation**: Backtesting with realistic latency and fill modeling
//! - **Production Monitoring**: Multi-channel alerting, audit trails, performance analytics
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   MARKET DISCOVERY SERVICE                       │
//! │         (Venue Scanning, Semantic Matching, Mapping)             │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │              CROSS-VENUE ORDER BOOK AGGREGATOR                   │
//! │    (Polymarket WS + Kalshi WS + Opinion WS → Unified Book)       │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │                  ARBITRAGE DETECTOR                              │
//! │  (Binary, Multi-Outcome, Cross-Market, Temporal, Cross-Venue)    │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │              CROSS-PLATFORM EXECUTION ENGINE                     │
//! │     (Leg Sequencing, Partial Fills, Hedge Retry, Unwind)         │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │                    RISK MANAGER                                  │
//! │      (Limits, Circuit Breaker, Kill Switch, P&L Tracking)        │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │               POSITION RECONCILIATION DAEMON                     │
//! │        (Drift Detection, Exchange Sync, Discrepancy Alerts)      │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │              MONITORING & ALERTING                               │
//! │    (Slack/Discord/Telegram/PagerDuty, Audit Trail, Analytics)    │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Cross-Platform Execution Flow
//!
//! 1. **Detection**: Arbitrage detector identifies cross-venue opportunity
//! 2. **Leg Selection**: Execute on more liquid venue first (liquidity score)
//! 3. **First Leg**: Submit order on primary venue, wait for fill
//! 4. **Hedge Leg**: Submit hedge order on secondary venue
//! 5. **Partial Fill Handling**: Retry with aggressive pricing or abort
//! 6. **Unwind**: If hedge fails, unwind first leg position
//! 7. **Reconciliation**: Verify positions match across venues

pub mod analytics;
pub mod api;
pub mod core;
pub mod discovery;
pub mod execution;
pub mod monitoring;
pub mod orderbook;
pub mod reconciliation;
pub mod risk;
pub mod simulation;
pub mod strategies;

// Re-exports for convenience
pub use analytics::{PnLTracker, StrategyAnalytics, VenueAnalytics, DashboardEndpoint, DashboardData};
pub use api::{ClobClient, GammaClient, Signer};
pub use core::{Config, Error, Event, EventBus, Result};
pub use discovery::{MarketMapper, MarketMapping, SemanticMatcher};
pub use execution::ExecutionEngine;
pub use monitoring::{HealthChecker, MetricsCollector};
pub use orderbook::OrderbookEngine;
pub use reconciliation::{PositionTracker, PositionReconciler, DriftDetector};
pub use risk::RiskManager;
pub use simulation::{SimulatedExecutor, BacktestResult, PerformanceMetrics};
pub use strategies::ArbitrageDetector;
