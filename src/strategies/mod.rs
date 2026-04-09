//! Arbitrage detection strategies.
//!
//! Implements multiple arbitrage detection algorithms:
//! - Binary market mispricing (single venue)
//! - Multi-outcome mispricing (single venue)
//! - Cross-market arbitrage (single venue)
//! - Temporal arbitrage (5-min markets)
//! - Cross-platform arbitrage (Polymarket ↔ Kalshi/Opinion)

pub mod binary;
pub mod multi_outcome;
pub mod cross_market;
pub mod temporal;
pub mod detector;
pub mod cross_platform;

pub use binary::BinaryArbitrageStrategy;
pub use multi_outcome::MultiOutcomeStrategy;
pub use cross_market::CrossMarketStrategy;
pub use temporal::TemporalStrategy;
pub use detector::ArbitrageDetector;
pub use cross_platform::{
    CrossPlatformStrategy, CrossPlatformStrategyConfig, MarketMapping,
    PolymarketMarketInfo, KalshiMarketInfo, OpinionMarketInfo,
    CrossVenuePriceSnapshot, VenuePrices,
};
