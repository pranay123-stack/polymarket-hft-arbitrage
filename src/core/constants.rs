//! Constants and contract addresses used throughout the bot.

use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ============================================================================
// Polymarket Contract Addresses (Polygon Mainnet)
// ============================================================================

/// CTF Exchange contract address
pub const CTF_EXCHANGE_ADDRESS: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8DB438C";

/// Neg Risk CTF Exchange address (for markets with negative risk)
pub const NEG_RISK_CTF_EXCHANGE_ADDRESS: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// Neg Risk Adapter address
pub const NEG_RISK_ADAPTER_ADDRESS: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

/// USDC contract address on Polygon
pub const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Conditional Tokens (CTF) contract address
pub const CONDITIONAL_TOKENS_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// Proxy Wallet Factory address
pub const PROXY_WALLET_FACTORY_ADDRESS: &str = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052";

// ============================================================================
// Chain Configuration
// ============================================================================

/// Polygon Mainnet Chain ID
pub const POLYGON_CHAIN_ID: u64 = 137;

/// Polygon block time (approximately 2 seconds)
pub const POLYGON_BLOCK_TIME_MS: u64 = 2000;

/// USDC decimals on Polygon
pub const USDC_DECIMALS: u8 = 6;

// ============================================================================
// API Endpoints
// ============================================================================

/// CLOB REST API base URL
pub const CLOB_API_URL: &str = "https://clob.polymarket.com";

/// CLOB WebSocket URL
pub const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// CLOB User WebSocket URL
pub const CLOB_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";

/// Gamma API URL (market data)
pub const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";

/// Strapi API URL (additional market data)
pub const STRAPI_API_URL: &str = "https://strapi-matic.poly.market";

// ============================================================================
// Trading Constants
// ============================================================================

/// Minimum order size in shares
pub static MIN_ORDER_SIZE: Lazy<Decimal> = Lazy::new(|| dec!(0.1));

/// Maximum order size in shares
pub static MAX_ORDER_SIZE: Lazy<Decimal> = Lazy::new(|| dec!(100000));

/// Tick size (minimum price increment)
pub static TICK_SIZE: Lazy<Decimal> = Lazy::new(|| dec!(0.001));

/// Minimum price
pub static MIN_PRICE: Lazy<Decimal> = Lazy::new(|| dec!(0.001));

/// Maximum price
pub static MAX_PRICE: Lazy<Decimal> = Lazy::new(|| dec!(0.999));

/// Polymarket fee rate (maker + taker combined is typically ~2%)
pub static FEE_RATE: Lazy<Decimal> = Lazy::new(|| dec!(0.02));

/// Maker rebate (if applicable)
pub static MAKER_REBATE: Lazy<Decimal> = Lazy::new(|| dec!(0.0));

// ============================================================================
// ERC1155 Event Signatures
// ============================================================================

/// TransferSingle event signature
/// keccak256("TransferSingle(address,address,address,uint256,uint256)")
pub const TRANSFER_SINGLE_EVENT_SIGNATURE: &str =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";

/// TransferBatch event signature
pub const TRANSFER_BATCH_EVENT_SIGNATURE: &str =
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

// ============================================================================
// API Rate Limits
// ============================================================================

/// Maximum requests per second to CLOB API
pub const CLOB_API_RATE_LIMIT: u32 = 10;

/// Maximum requests per second to Gamma API
pub const GAMMA_API_RATE_LIMIT: u32 = 5;

/// WebSocket ping interval in seconds
pub const WS_PING_INTERVAL_SECS: u64 = 30;

/// WebSocket reconnect delay (base) in milliseconds
pub const WS_RECONNECT_DELAY_MS: u64 = 1000;

/// Maximum WebSocket reconnect delay in milliseconds
pub const WS_MAX_RECONNECT_DELAY_MS: u64 = 30000;

// ============================================================================
// Timing Constants
// ============================================================================

/// Order book snapshot staleness threshold in milliseconds
pub const ORDERBOOK_STALE_THRESHOLD_MS: u64 = 5000;

/// Price staleness threshold in milliseconds
pub const PRICE_STALE_THRESHOLD_MS: u64 = 10000;

/// Arbitrage opportunity expiration in milliseconds
pub const ARBITRAGE_EXPIRATION_MS: u64 = 2000;

/// Default order timeout in seconds
pub const DEFAULT_ORDER_TIMEOUT_SECS: u64 = 60;

/// Transaction confirmation timeout in seconds
pub const TX_CONFIRMATION_TIMEOUT_SECS: u64 = 120;

// ============================================================================
// Risk Parameters (Defaults)
// ============================================================================

/// Default maximum position size in USDC
pub static DEFAULT_MAX_POSITION_SIZE: Lazy<Decimal> = Lazy::new(|| dec!(500));

/// Default maximum total exposure in USDC
pub static DEFAULT_MAX_EXPOSURE: Lazy<Decimal> = Lazy::new(|| dec!(2000));

/// Default maximum daily loss in USDC
pub static DEFAULT_MAX_DAILY_LOSS: Lazy<Decimal> = Lazy::new(|| dec!(100));

/// Default maximum drawdown percentage
pub static DEFAULT_MAX_DRAWDOWN_PCT: Lazy<Decimal> = Lazy::new(|| dec!(10));

/// Default slippage tolerance
pub static DEFAULT_SLIPPAGE_TOLERANCE: Lazy<Decimal> = Lazy::new(|| dec!(0.01));

// ============================================================================
// Binary Arbitrage Constants
// ============================================================================

/// Minimum mispricing threshold for binary markets (YES + NO deviation from 1.0)
pub static BINARY_MISPRICING_THRESHOLD: Lazy<Decimal> = Lazy::new(|| dec!(0.02));

/// Minimum expected profit for binary arbitrage (after fees)
pub static MIN_BINARY_PROFIT: Lazy<Decimal> = Lazy::new(|| dec!(0.01));

// ============================================================================
// 5-Minute BTC Market Constants
// ============================================================================

/// BTC 5-minute market slug prefix
pub const BTC_5MIN_SLUG_PREFIX: &str = "btc-updown-5m-";

/// ETH 5-minute market slug prefix
pub const ETH_5MIN_SLUG_PREFIX: &str = "eth-updown-5m-";

/// 5-minute market duration in seconds
pub const FIVE_MIN_MARKET_DURATION_SECS: u64 = 300;

/// Time before market close to stop trading (seconds)
pub const MARKET_CLOSE_BUFFER_SECS: u64 = 30;

// ============================================================================
// Metrics Constants
// ============================================================================

/// Metrics namespace
pub const METRICS_NAMESPACE: &str = "polymarket_hft";

/// Health check endpoint path
pub const HEALTH_CHECK_PATH: &str = "/health";

/// Metrics endpoint path
pub const METRICS_PATH: &str = "/metrics";

// ============================================================================
// Database Constants
// ============================================================================

/// Maximum database connection pool size
pub const DB_MAX_CONNECTIONS: u32 = 10;

/// Database connection timeout in seconds
pub const DB_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Redis key prefix
pub const REDIS_KEY_PREFIX: &str = "polymarket_hft:";

/// Redis cache TTL in seconds
pub const REDIS_CACHE_TTL_SECS: u64 = 300;

// ============================================================================
// Logging Constants
// ============================================================================

/// Log field for market ID
pub const LOG_FIELD_MARKET_ID: &str = "market_id";

/// Log field for order ID
pub const LOG_FIELD_ORDER_ID: &str = "order_id";

/// Log field for trade ID
pub const LOG_FIELD_TRADE_ID: &str = "trade_id";

/// Log field for opportunity ID
pub const LOG_FIELD_OPPORTUNITY_ID: &str = "opportunity_id";

/// Log field for latency
pub const LOG_FIELD_LATENCY_MS: &str = "latency_ms";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants_values() {
        assert_eq!(POLYGON_CHAIN_ID, 137);
        assert!(*MIN_ORDER_SIZE < *MAX_ORDER_SIZE);
        assert!(*MIN_PRICE < *MAX_PRICE);
    }

    #[test]
    fn test_addresses_format() {
        // All addresses should be 42 characters (0x + 40 hex chars)
        assert_eq!(CTF_EXCHANGE_ADDRESS.len(), 42);
        assert_eq!(USDC_ADDRESS.len(), 42);
        assert!(CTF_EXCHANGE_ADDRESS.starts_with("0x"));
    }
}
