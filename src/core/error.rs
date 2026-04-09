//! Error handling for the HFT arbitrage bot.
//!
//! Provides a unified error type with context for debugging and monitoring.

use std::fmt;
use thiserror::Error;

/// Result type alias using our Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the application
#[derive(Error, Debug)]
pub enum Error {
    // Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    // API errors
    #[error("API error: {0}")]
    Api(#[from] ApiError),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    // Blockchain errors
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] BlockchainError),

    // Trading errors
    #[error("Trading error: {0}")]
    Trading(#[from] TradingError),

    // Risk errors
    #[error("Risk limit breached: {0}")]
    Risk(#[from] RiskError),

    // Strategy errors
    #[error("Strategy error: {0}")]
    Strategy(String),

    // Database errors
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    // Serialization errors
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    // Cryptographic errors
    #[error("Cryptographic error: {0}")]
    Crypto(String),

    // System errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel send error")]
    ChannelSend,

    #[error("Channel receive error")]
    ChannelRecv,

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Parse error: {0}")]
    Parse(String),

    // Internal errors
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Kill switch activated")]
    KillSwitchActivated,
}

impl Error {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Http(_)
                | Error::WebSocket(_)
                | Error::Timeout(_)
                | Error::Api(ApiError::RateLimited { .. })
                | Error::Api(ApiError::ServiceUnavailable)
                | Error::Blockchain(BlockchainError::RpcError(_))
        )
    }

    /// Check if this is a critical error requiring immediate attention
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            Error::Risk(_)
                | Error::CircuitBreakerOpen
                | Error::KillSwitchActivated
                | Error::Blockchain(BlockchainError::InsufficientFunds)
                | Error::Trading(TradingError::ExecutionFailed { .. })
        )
    }

    /// Get error code for metrics/logging
    pub fn error_code(&self) -> &'static str {
        match self {
            Error::Config(_) => "CONFIG_ERROR",
            Error::Api(_) => "API_ERROR",
            Error::Http(_) => "HTTP_ERROR",
            Error::WebSocket(_) => "WEBSOCKET_ERROR",
            Error::Blockchain(_) => "BLOCKCHAIN_ERROR",
            Error::Trading(_) => "TRADING_ERROR",
            Error::Risk(_) => "RISK_ERROR",
            Error::Strategy(_) => "STRATEGY_ERROR",
            Error::Database(_) => "DATABASE_ERROR",
            Error::Redis(_) => "REDIS_ERROR",
            Error::Json(_) => "JSON_ERROR",
            Error::Crypto(_) => "CRYPTO_ERROR",
            Error::Io(_) => "IO_ERROR",
            Error::ChannelSend => "CHANNEL_SEND_ERROR",
            Error::ChannelRecv => "CHANNEL_RECV_ERROR",
            Error::Timeout(_) => "TIMEOUT_ERROR",
            Error::Parse(_) => "PARSE_ERROR",
            Error::Internal(_) => "INTERNAL_ERROR",
            Error::NotFound(_) => "NOT_FOUND",
            Error::InvalidState(_) => "INVALID_STATE",
            Error::CircuitBreakerOpen => "CIRCUIT_BREAKER_OPEN",
            Error::KillSwitchActivated => "KILL_SWITCH_ACTIVATED",
        }
    }
}

/// API-specific errors
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Invalid API response: {0}")]
    InvalidResponse(String),

    #[error("Rate limited, retry after {retry_after_secs} seconds")]
    RateLimited { retry_after_secs: u64 },

    #[error("Service unavailable")]
    ServiceUnavailable,

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Order rejected: {reason}")]
    OrderRejected { reason: String },

    #[error("Insufficient balance: required {required}, available {available}")]
    InsufficientBalance {
        required: String,
        available: String,
    },

    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("Order not found: {0}")]
    OrderNotFound(String),

    #[error("Signature invalid")]
    InvalidSignature,
}

/// Blockchain-specific errors
#[derive(Error, Debug)]
pub enum BlockchainError {
    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Transaction reverted: {0}")]
    TransactionReverted(String),

    #[error("Insufficient funds for gas")]
    InsufficientFunds,

    #[error("Nonce too low")]
    NonceTooLow,

    #[error("Gas price too high: {0} gwei")]
    GasPriceTooHigh(u64),

    #[error("Contract error: {0}")]
    ContractError(String),

    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Chain reorg detected at block {0}")]
    ChainReorg(u64),
}

/// Trading-specific errors
#[derive(Error, Debug)]
pub enum TradingError {
    #[error("Order execution failed: {reason}, order_id: {order_id}")]
    ExecutionFailed { order_id: String, reason: String },

    #[error("Order cancelled: {order_id}")]
    OrderCancelled { order_id: String },

    #[error("Order expired: {order_id}")]
    OrderExpired { order_id: String },

    #[error("Partial fill not allowed: filled {filled}, expected {expected}")]
    PartialFillRejected { filled: String, expected: String },

    #[error("Slippage exceeded: expected {expected}, actual {actual}")]
    SlippageExceeded { expected: String, actual: String },

    #[error("Insufficient liquidity: available {available}, required {required}")]
    InsufficientLiquidity { available: String, required: String },

    #[error("Market closed: {market_id}")]
    MarketClosed { market_id: String },

    #[error("Price stale: last update {last_update_secs}s ago")]
    StalePrice { last_update_secs: u64 },

    #[error("Arbitrage opportunity expired")]
    OpportunityExpired,

    #[error("Atomic execution failed: {0}")]
    AtomicExecutionFailed(String),
}

/// Risk-specific errors
#[derive(Error, Debug)]
pub enum RiskError {
    #[error("Position size limit exceeded: {current} > {limit}")]
    PositionSizeExceeded { current: String, limit: String },

    #[error("Total exposure limit exceeded: {current} > {limit}")]
    ExposureLimitExceeded { current: String, limit: String },

    #[error("Daily loss limit exceeded: {current} > {limit}")]
    DailyLossExceeded { current: String, limit: String },

    #[error("Drawdown limit exceeded: {current_pct}% > {limit_pct}%")]
    DrawdownExceeded { current_pct: String, limit_pct: String },

    #[error("Too many open positions: {current} > {limit}")]
    TooManyPositions { current: u32, limit: u32 },

    #[error("Order rate limit exceeded: {current}/min > {limit}/min")]
    OrderRateLimitExceeded { current: u32, limit: u32 },

    #[error("Consecutive losses exceeded: {count} > {limit}")]
    ConsecutiveLossesExceeded { count: u32, limit: u32 },

    #[error("High correlation risk: {correlation}")]
    HighCorrelation { correlation: String },

    #[error("Market volatility too high")]
    HighVolatility,
}

/// Extension trait for adding context to errors
pub trait ErrorContext<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T>;
    fn with_context<F, C>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: fmt::Display;
}

impl<T, E: Into<Error>> ErrorContext<T> for std::result::Result<T, E> {
    fn context(self, ctx: impl fmt::Display) -> Result<T> {
        self.map_err(|e| {
            let err: Error = e.into();
            Error::Internal(format!("{}: {}", ctx, err))
        })
    }

    fn with_context<F, C>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: fmt::Display,
    {
        self.map_err(|e| {
            let err: Error = e.into();
            Error::Internal(format!("{}: {}", f(), err))
        })
    }
}

impl<T> ErrorContext<T> for Option<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T> {
        self.ok_or_else(|| Error::NotFound(ctx.to_string()))
    }

    fn with_context<F, C>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: fmt::Display,
    {
        self.ok_or_else(|| Error::NotFound(f().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retryable() {
        let rate_limited = Error::Api(ApiError::RateLimited { retry_after_secs: 5 });
        assert!(rate_limited.is_retryable());

        let config_err = Error::Config("bad config".to_string());
        assert!(!config_err.is_retryable());
    }

    #[test]
    fn test_error_critical() {
        let risk_err = Error::Risk(RiskError::DailyLossExceeded {
            current: "150".to_string(),
            limit: "100".to_string(),
        });
        assert!(risk_err.is_critical());

        let parse_err = Error::Parse("bad data".to_string());
        assert!(!parse_err.is_critical());
    }

    #[test]
    fn test_error_code() {
        let err = Error::CircuitBreakerOpen;
        assert_eq!(err.error_code(), "CIRCUIT_BREAKER_OPEN");
    }
}
