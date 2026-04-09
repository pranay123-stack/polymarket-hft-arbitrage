//! Configuration management for the HFT arbitrage bot.
//!
//! Supports loading from:
//! - Environment variables
//! - Configuration files (TOML/JSON)
//! - Command line arguments
//!
//! Secrets are loaded from environment variables only for security.

use crate::core::error::{Error, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use url::Url;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Trading mode
    pub mode: TradingMode,

    /// API configuration
    pub api: ApiConfig,

    /// Blockchain configuration
    pub blockchain: BlockchainConfig,

    /// Trading parameters
    pub trading: TradingConfig,

    /// Risk management settings
    pub risk: RiskConfig,

    /// Strategy configuration
    pub strategy: StrategyConfig,

    /// Database configuration
    pub database: DatabaseConfig,

    /// Monitoring configuration
    pub monitoring: MonitoringConfig,

    /// Logging configuration
    pub logging: LoggingConfig,
}

impl Config {
    /// Load configuration from file and environment
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut builder = config::Config::builder();

        // Load from file if provided
        if let Some(path) = path {
            builder = builder.add_source(config::File::from(path));
        }

        // Override with environment variables
        builder = builder.add_source(
            config::Environment::with_prefix("POLYBOT")
                .separator("__")
                .try_parsing(true)
        );

        let config = builder.build()
            .map_err(|e| Error::Config(format!("Failed to build config: {}", e)))?;

        let mut cfg: Config = config.try_deserialize()
            .map_err(|e| Error::Config(format!("Failed to deserialize config: {}", e)))?;

        // Load secrets from environment
        cfg.load_secrets()?;

        // Validate configuration
        cfg.validate()?;

        Ok(cfg)
    }

    /// Load secrets from environment variables
    fn load_secrets(&mut self) -> Result<()> {
        // Private key (required)
        self.api.private_key = std::env::var("POLYBOT_PRIVATE_KEY")
            .map_err(|_| Error::Config("POLYBOT_PRIVATE_KEY environment variable not set".into()))?;

        // API credentials (optional, can be derived)
        if let Ok(api_key) = std::env::var("POLYBOT_API_KEY") {
            self.api.api_key = Some(api_key);
        }
        if let Ok(api_secret) = std::env::var("POLYBOT_API_SECRET") {
            self.api.api_secret = Some(api_secret);
        }
        if let Ok(api_passphrase) = std::env::var("POLYBOT_API_PASSPHRASE") {
            self.api.api_passphrase = Some(api_passphrase);
        }

        // Kalshi credentials (optional)
        if let Ok(email) = std::env::var("KALSHI_EMAIL") {
            self.api.kalshi.email = email;
        }
        if let Ok(password) = std::env::var("KALSHI_PASSWORD") {
            self.api.kalshi.password = password;
        }

        // Opinion credentials (optional)
        if let Ok(api_key) = std::env::var("OPINION_API_KEY") {
            self.api.opinion.api_key = api_key;
        }
        if let Ok(api_secret) = std::env::var("OPINION_API_SECRET") {
            self.api.opinion.api_secret = api_secret;
        }

        // Database password
        if let Ok(db_password) = std::env::var("POLYBOT_DB_PASSWORD") {
            self.database.password = Some(db_password);
        }

        // Redis password
        if let Ok(redis_password) = std::env::var("POLYBOT_REDIS_PASSWORD") {
            self.database.redis_password = Some(redis_password);
        }

        Ok(())
    }

    /// Validate configuration
    fn validate(&self) -> Result<()> {
        // Validate private key format
        if self.api.private_key.is_empty() {
            return Err(Error::Config("Private key cannot be empty".into()));
        }

        // Validate risk limits
        if self.risk.max_position_size <= Decimal::ZERO {
            return Err(Error::Config("max_position_size must be positive".into()));
        }
        if self.risk.max_daily_loss <= Decimal::ZERO {
            return Err(Error::Config("max_daily_loss must be positive".into()));
        }
        if self.risk.max_drawdown_pct <= Decimal::ZERO || self.risk.max_drawdown_pct > Decimal::from(100) {
            return Err(Error::Config("max_drawdown_pct must be between 0 and 100".into()));
        }

        // Validate trading parameters
        if self.trading.min_profit_threshold < Decimal::ZERO {
            return Err(Error::Config("min_profit_threshold cannot be negative".into()));
        }

        // Validate URLs
        self.api.clob_http_url.parse::<Url>()
            .map_err(|_| Error::Config("Invalid CLOB HTTP URL".into()))?;
        self.api.clob_ws_url.parse::<Url>()
            .map_err(|_| Error::Config("Invalid CLOB WebSocket URL".into()))?;

        Ok(())
    }

    /// Check if running in simulation mode
    pub fn is_simulation(&self) -> bool {
        matches!(self.mode, TradingMode::Simulation | TradingMode::Paper)
    }

    /// Check if running in production mode
    pub fn is_production(&self) -> bool {
        matches!(self.mode, TradingMode::Live)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: TradingMode::Paper,
            api: ApiConfig::default(),
            blockchain: BlockchainConfig::default(),
            trading: TradingConfig::default(),
            risk: RiskConfig::default(),
            strategy: StrategyConfig::default(),
            database: DatabaseConfig::default(),
            monitoring: MonitoringConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

/// Trading mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TradingMode {
    /// Simulation with historical data
    Simulation,
    /// Paper trading with live data but no real orders
    #[default]
    Paper,
    /// Live trading with real money
    Live,
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Private key for signing (loaded from env)
    #[serde(skip)]
    pub private_key: String,

    /// API key (optional, can be derived)
    #[serde(skip)]
    pub api_key: Option<String>,

    /// API secret (optional, can be derived)
    #[serde(skip)]
    pub api_secret: Option<String>,

    /// API passphrase (optional)
    #[serde(skip)]
    pub api_passphrase: Option<String>,

    /// CLOB REST API URL
    pub clob_http_url: String,

    /// CLOB WebSocket URL
    pub clob_ws_url: String,

    /// Gamma API URL for market data
    pub gamma_api_url: String,

    /// Request timeout in milliseconds
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,

    /// Maximum retries for failed requests
    pub max_retries: u32,

    /// Rate limit (requests per second)
    pub rate_limit_rps: u32,

    /// Use proxy wallet
    pub use_proxy_wallet: bool,

    /// Proxy wallet address (if different from derived)
    pub proxy_wallet_address: Option<String>,

    /// Kalshi API configuration
    pub kalshi: KalshiApiConfig,

    /// Opinion API configuration
    pub opinion: OpinionApiConfig,
}

/// Kalshi API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiApiConfig {
    /// Kalshi API base URL
    pub base_url: String,

    /// Email (loaded from env)
    #[serde(skip)]
    pub email: String,

    /// Password (loaded from env)
    #[serde(skip)]
    pub password: String,

    /// Request timeout (ms)
    pub timeout_ms: u64,

    /// Enable Kalshi integration
    pub enabled: bool,
}

impl Default for KalshiApiConfig {
    fn default() -> Self {
        Self {
            base_url: "https://trading-api.kalshi.com/trade-api/v2".to_string(),
            email: String::new(),
            password: String::new(),
            timeout_ms: 5000,
            enabled: true,
        }
    }
}

/// Opinion API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionApiConfig {
    /// Opinion API base URL
    pub base_url: String,

    /// API key (loaded from env)
    #[serde(skip)]
    pub api_key: String,

    /// API secret (loaded from env)
    #[serde(skip)]
    pub api_secret: String,

    /// Request timeout (ms)
    pub timeout_ms: u64,

    /// Commission rate on winnings
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_rate: Decimal,

    /// Enable Opinion integration
    pub enabled: bool,
}

impl Default for OpinionApiConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.opinion.exchange/v1".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            timeout_ms: 5000,
            commission_rate: Decimal::from_str_exact("0.02").unwrap(),
            enabled: true,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            private_key: String::new(),
            api_key: None,
            api_secret: None,
            api_passphrase: None,
            clob_http_url: "https://clob.polymarket.com".to_string(),
            clob_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
            gamma_api_url: "https://gamma-api.polymarket.com".to_string(),
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
            rate_limit_rps: 10,
            use_proxy_wallet: true,
            proxy_wallet_address: None,
            kalshi: KalshiApiConfig::default(),
            opinion: OpinionApiConfig::default(),
        }
    }
}

/// Blockchain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainConfig {
    /// Polygon RPC HTTP URL
    pub polygon_rpc_http: String,

    /// Polygon RPC WebSocket URL
    pub polygon_rpc_ws: String,

    /// Chain ID (137 for Polygon mainnet)
    pub chain_id: u64,

    /// CTF Exchange contract address
    pub ctf_exchange_address: String,

    /// Neg Risk CTF Exchange address
    pub neg_risk_ctf_exchange_address: String,

    /// USDC contract address
    pub usdc_address: String,

    /// Conditional Tokens contract address
    pub conditional_tokens_address: String,

    /// Gas price strategy
    pub gas_strategy: GasStrategy,

    /// Maximum gas price in gwei
    pub max_gas_price_gwei: u64,

    /// Confirmation blocks to wait
    pub confirmation_blocks: u64,
}

impl Default for BlockchainConfig {
    fn default() -> Self {
        Self {
            polygon_rpc_http: "https://polygon-rpc.com".to_string(),
            polygon_rpc_ws: "wss://polygon-bor-rpc.publicnode.com".to_string(),
            chain_id: 137,
            ctf_exchange_address: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8DB438C".to_string(),
            neg_risk_ctf_exchange_address: "0xC5d563A36AE78145C45a50134d48A1215220f80a".to_string(),
            usdc_address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string(),
            conditional_tokens_address: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".to_string(),
            gas_strategy: GasStrategy::Fast,
            max_gas_price_gwei: 500,
            confirmation_blocks: 1,
        }
    }
}

/// Gas price strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GasStrategy {
    /// Use slow gas price
    Slow,
    /// Use standard gas price
    Standard,
    /// Use fast gas price
    #[default]
    Fast,
    /// Use fastest (instant) gas price
    Instant,
    /// Fixed gas price
    Fixed { gwei: u64 },
}

/// Trading configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    /// Minimum profit threshold (in USDC) to execute a trade
    #[serde(with = "rust_decimal::serde::str")]
    pub min_profit_threshold: Decimal,

    /// Minimum profit percentage to execute
    #[serde(with = "rust_decimal::serde::str")]
    pub min_profit_pct: Decimal,

    /// Default order size in USDC
    #[serde(with = "rust_decimal::serde::str")]
    pub default_order_size: Decimal,

    /// Maximum order size in USDC
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_size: Decimal,

    /// Minimum liquidity required to trade (in shares)
    #[serde(with = "rust_decimal::serde::str")]
    pub min_liquidity: Decimal,

    /// Maximum spread to accept (in decimal, e.g., 0.05 = 5%)
    #[serde(with = "rust_decimal::serde::str")]
    pub max_spread: Decimal,

    /// Order expiration time
    #[serde(with = "humantime_serde")]
    pub order_expiration: Duration,

    /// Time to wait before cancelling unfilled orders
    #[serde(with = "humantime_serde")]
    pub order_timeout: Duration,

    /// Enable atomic execution (all-or-nothing)
    pub atomic_execution: bool,

    /// Slippage tolerance (decimal)
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_tolerance: Decimal,

    /// Fee rate (Polymarket takes ~2%)
    #[serde(with = "rust_decimal::serde::str")]
    pub fee_rate: Decimal,
}

impl Default for TradingConfig {
    fn default() -> Self {
        Self {
            min_profit_threshold: Decimal::from_str_exact("0.10").unwrap(),
            min_profit_pct: Decimal::from_str_exact("0.5").unwrap(),
            default_order_size: Decimal::from(10),
            max_order_size: Decimal::from(100),
            min_liquidity: Decimal::from(50),
            max_spread: Decimal::from_str_exact("0.10").unwrap(),
            order_expiration: Duration::from_secs(300),
            order_timeout: Duration::from_secs(60),
            atomic_execution: true,
            slippage_tolerance: Decimal::from_str_exact("0.01").unwrap(),
            fee_rate: Decimal::from_str_exact("0.02").unwrap(),
        }
    }
}

/// Risk management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum position size per market (in USDC)
    #[serde(with = "rust_decimal::serde::str")]
    pub max_position_size: Decimal,

    /// Maximum total exposure (in USDC)
    #[serde(with = "rust_decimal::serde::str")]
    pub max_total_exposure: Decimal,

    /// Maximum daily loss (in USDC)
    #[serde(with = "rust_decimal::serde::str")]
    pub max_daily_loss: Decimal,

    /// Maximum drawdown percentage
    #[serde(with = "rust_decimal::serde::str")]
    pub max_drawdown_pct: Decimal,

    /// Maximum number of open positions
    pub max_open_positions: u32,

    /// Maximum number of orders per minute
    pub max_orders_per_minute: u32,

    /// Maximum correlation between positions
    #[serde(with = "rust_decimal::serde::str")]
    pub max_correlation: Decimal,

    /// Enable circuit breaker
    pub circuit_breaker_enabled: bool,

    /// Circuit breaker cooldown period
    #[serde(with = "humantime_serde")]
    pub circuit_breaker_cooldown: Duration,

    /// Maximum consecutive losses before circuit breaker
    pub max_consecutive_losses: u32,

    /// Kill switch enabled
    pub kill_switch_enabled: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_size: Decimal::from(500),
            max_total_exposure: Decimal::from(2000),
            max_daily_loss: Decimal::from(100),
            max_drawdown_pct: Decimal::from(10),
            max_open_positions: 10,
            max_orders_per_minute: 60,
            max_correlation: Decimal::from_str_exact("0.8").unwrap(),
            circuit_breaker_enabled: true,
            circuit_breaker_cooldown: Duration::from_secs(300),
            max_consecutive_losses: 5,
            kill_switch_enabled: true,
        }
    }
}

/// Strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Enable binary mispricing arbitrage
    pub binary_mispricing: bool,

    /// Enable multi-outcome arbitrage
    pub multi_outcome: bool,

    /// Enable cross-market arbitrage
    pub cross_market: bool,

    /// Enable temporal arbitrage (5-min markets)
    pub temporal: bool,

    /// Enable statistical arbitrage
    pub statistical: bool,

    /// Enable cross-platform arbitrage (Polymarket ↔ Kalshi/Opinion)
    pub cross_platform: bool,

    /// Market slugs to focus on (empty = all)
    pub focus_markets: Vec<String>,

    /// Market slugs to exclude
    pub exclude_markets: Vec<String>,

    /// Minimum market volume to consider
    #[serde(with = "rust_decimal::serde::str")]
    pub min_market_volume: Decimal,

    /// Scan interval for opportunities
    #[serde(with = "humantime_serde")]
    pub scan_interval: Duration,

    /// Binary mispricing threshold (sum deviation from 1.0)
    #[serde(with = "rust_decimal::serde::str")]
    pub binary_threshold: Decimal,

    /// Cross-market correlation threshold
    #[serde(with = "rust_decimal::serde::str")]
    pub cross_market_threshold: Decimal,

    /// Cross-platform configuration
    pub cross_platform_config: CrossPlatformStrategyConfig,
}

/// Cross-platform (multi-venue) strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossPlatformStrategyConfig {
    /// Minimum profit in USD
    #[serde(with = "rust_decimal::serde::str")]
    pub min_profit_usd: Decimal,

    /// Minimum profit percentage
    #[serde(with = "rust_decimal::serde::str")]
    pub min_profit_pct: Decimal,

    /// Maximum position size
    #[serde(with = "rust_decimal::serde::str")]
    pub max_position_size: Decimal,

    /// Maximum hedge slippage before abort
    #[serde(with = "rust_decimal::serde::str")]
    pub max_hedge_slippage: Decimal,

    /// Minimum first leg fill percentage
    #[serde(with = "rust_decimal::serde::str")]
    pub min_first_leg_fill_pct: Decimal,

    /// First leg timeout (ms)
    pub first_leg_timeout_ms: u64,

    /// Hedge leg timeout (ms)
    pub hedge_leg_timeout_ms: u64,

    /// Maximum hedge retries
    pub max_hedge_retries: u32,

    /// Hedge price aggression (added to price to improve fill)
    #[serde(with = "rust_decimal::serde::str")]
    pub hedge_price_aggression: Decimal,

    /// Use IOC for hedge orders
    pub use_ioc_for_hedge: bool,

    /// Enable automatic state recovery
    pub enable_state_recovery: bool,

    /// Kalshi enabled
    pub kalshi_enabled: bool,

    /// Opinion enabled
    pub opinion_enabled: bool,
}

impl Default for CrossPlatformStrategyConfig {
    fn default() -> Self {
        Self {
            min_profit_usd: Decimal::from_str_exact("1.0").unwrap(),
            min_profit_pct: Decimal::from_str_exact("0.005").unwrap(),
            max_position_size: Decimal::from(1000),
            max_hedge_slippage: Decimal::from_str_exact("0.03").unwrap(),
            min_first_leg_fill_pct: Decimal::from_str_exact("0.5").unwrap(),
            first_leg_timeout_ms: 3000,
            hedge_leg_timeout_ms: 5000,
            max_hedge_retries: 3,
            hedge_price_aggression: Decimal::from_str_exact("0.01").unwrap(),
            use_ioc_for_hedge: true,
            enable_state_recovery: true,
            kalshi_enabled: true,
            opinion_enabled: true,
        }
    }
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            binary_mispricing: true,
            multi_outcome: true,
            cross_market: true,
            temporal: true,
            statistical: false,
            cross_platform: true,
            focus_markets: Vec::new(),
            exclude_markets: Vec::new(),
            min_market_volume: Decimal::from(1000),
            scan_interval: Duration::from_millis(100),
            binary_threshold: Decimal::from_str_exact("0.02").unwrap(),
            cross_market_threshold: Decimal::from_str_exact("0.05").unwrap(),
            cross_platform_config: CrossPlatformStrategyConfig::default(),
        }
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL host
    pub host: String,

    /// PostgreSQL port
    pub port: u16,

    /// Database name
    pub database: String,

    /// Username
    pub username: String,

    /// Password (loaded from env)
    #[serde(skip)]
    pub password: Option<String>,

    /// Maximum connections
    pub max_connections: u32,

    /// Redis URL
    pub redis_url: String,

    /// Redis password (loaded from env)
    #[serde(skip)]
    pub redis_password: Option<String>,

    /// Enable database persistence
    pub enabled: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "polymarket_hft".to_string(),
            username: "polybot".to_string(),
            password: None,
            max_connections: 10,
            redis_url: "redis://localhost:6379".to_string(),
            redis_password: None,
            enabled: true,
        }
    }
}

impl DatabaseConfig {
    /// Build PostgreSQL connection string
    pub fn postgres_url(&self) -> String {
        let password = self.password.as_deref().unwrap_or("");
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, password, self.host, self.port, self.database
        )
    }

    /// Build Redis connection string
    pub fn redis_url_with_auth(&self) -> String {
        if let Some(ref password) = self.redis_password {
            self.redis_url.replace("redis://", &format!("redis://:{}@", password))
        } else {
            self.redis_url.clone()
        }
    }
}

/// Monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable Prometheus metrics
    pub prometheus_enabled: bool,

    /// Prometheus metrics port
    pub prometheus_port: u16,

    /// Enable dashboard
    pub dashboard_enabled: bool,

    /// Dashboard port
    pub dashboard_port: u16,

    /// Health check port
    pub health_port: u16,

    /// Metrics collection interval
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,

    /// Alert webhook URL (Discord, Slack, etc.)
    pub alert_webhook_url: Option<String>,

    /// Alert on trade execution
    pub alert_on_trade: bool,

    /// Alert on error
    pub alert_on_error: bool,

    /// Alert on circuit breaker trigger
    pub alert_on_circuit_breaker: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            prometheus_enabled: true,
            prometheus_port: 9090,
            dashboard_enabled: true,
            dashboard_port: 8080,
            health_port: 8081,
            metrics_interval: Duration::from_secs(10),
            alert_webhook_url: None,
            alert_on_trade: true,
            alert_on_error: true,
            alert_on_circuit_breaker: true,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,

    /// Log format (json, pretty)
    pub format: LogFormat,

    /// Log to file
    pub file_enabled: bool,

    /// Log file path
    pub file_path: String,

    /// Log rotation (daily, hourly, size)
    pub rotation: LogRotation,

    /// Maximum log file size in MB (for size-based rotation)
    pub max_size_mb: u64,

    /// Number of log files to retain
    pub retention_count: u32,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
            file_enabled: true,
            file_path: "./logs/polymarket-hft.log".to_string(),
            rotation: LogRotation::Daily,
            max_size_mb: 100,
            retention_count: 7,
        }
    }
}

/// Log format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Json,
    Pretty,
    Compact,
}

/// Log rotation strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogRotation {
    #[default]
    Daily,
    Hourly,
    Size,
    Never,
}

// Custom humantime serde module for Duration
mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = humantime::format_duration(*duration).to_string();
        s.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.is_simulation());
        assert!(!config.is_production());
    }

    #[test]
    fn test_postgres_url() {
        let mut db_config = DatabaseConfig::default();
        db_config.password = Some("secret".to_string());

        let url = db_config.postgres_url();
        assert!(url.contains("secret"));
        assert!(url.contains("polymarket_hft"));
    }
}
