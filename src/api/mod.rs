//! API module for multi-venue prediction market integration.
//!
//! Provides:
//! - Polymarket CLOB API client for order management
//! - Gamma API client for market data
//! - WebSocket clients for real-time updates
//! - Authentication and signing utilities
//! - Kalshi API client for cross-platform arbitrage
//! - Opinion API client for cross-platform arbitrage

pub mod clob;
pub mod gamma;
pub mod auth;
pub mod websocket;
pub mod rate_limiter;
pub mod kalshi;
pub mod opinion;

pub use clob::ClobClient;
pub use gamma::GammaClient;
pub use auth::Signer;
pub use websocket::{MarketWebSocket, UserWebSocket};
pub use rate_limiter::RateLimiter;
pub use kalshi::{KalshiClient, KalshiConfig, KalshiMarket, KalshiOrder, KalshiOrderbook};
pub use opinion::{OpinionClient, OpinionConfig, OpinionMarket, OpinionOrder, OpinionAction};
