//! Polymarket HFT Arbitrage Bot - Main Entry Point
//!
//! Production-grade arbitrage bot for Polymarket prediction markets.

use anyhow::Result;
use clap::{Parser, Subcommand};
use polymarket_hft::{
    api::{ClobClient, GammaClient, MarketWebSocket, Signer, UserWebSocket},
    core::{Config, EventBus},
    execution::ExecutionEngine,
    monitoring::{HealthChecker, MetricsCollector},
    orderbook::OrderbookEngine,
    risk::RiskManager,
    strategies::ArbitrageDetector,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{error, info, warn, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Polymarket HFT Arbitrage Bot
#[derive(Parser)]
#[command(name = "polymarket-hft")]
#[command(author = "Pranay <pranay@hft.dev>")]
#[command(version)]
#[command(about = "Production-grade HFT arbitrage bot for Polymarket", long_about = None)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Run in dry-run mode (no real trades)
    #[arg(long)]
    dry_run: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the arbitrage bot
    Run,

    /// Scan for arbitrage opportunities without executing
    Scan {
        /// Number of scans to perform
        #[arg(short, long, default_value = "1")]
        count: u32,

        /// Delay between scans in seconds
        #[arg(short, long, default_value = "5")]
        delay: u64,
    },

    /// Show current market status
    Status,

    /// Check account balance and positions
    Balance,

    /// Redeem winning positions
    Redeem {
        /// Market ID to redeem
        #[arg(short, long)]
        market: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(&cli.log_level);

    info!("Polymarket HFT Arbitrage Bot v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = Config::load(cli.config.as_deref())?;

    // Override mode if dry-run specified
    let config = if cli.dry_run {
        Config {
            mode: polymarket_hft::core::config::TradingMode::Paper,
            ..config
        }
    } else {
        config
    };

    info!("Running in {:?} mode", config.mode);

    // Execute command
    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => run_bot(config).await,
        Commands::Scan { count, delay } => scan_opportunities(config, count, delay).await,
        Commands::Status => show_status(config).await,
        Commands::Balance => show_balance(config).await,
        Commands::Redeem { market } => redeem_positions(config, market).await,
    }
}

/// Initialize logging with the specified level
fn init_logging(level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(true).with_thread_ids(true))
        .init();
}

/// Run the main arbitrage bot
async fn run_bot(config: Config) -> Result<()> {
    info!("Initializing bot components...");

    // Create event bus
    let event_bus = EventBus::new(10000);

    // Initialize signer
    let signer = Signer::new(&config.api.private_key)?;
    info!("Wallet address: {}", signer.address_string());

    // Initialize API clients
    let clob_client = Arc::new(ClobClient::new(
        &config.api,
        signer.clone(),
        config.blockchain.chain_id,
    )?);

    let gamma_client = Arc::new(GammaClient::new(&config.api)?);

    // Initialize orderbook engine
    let orderbook_engine = Arc::new(OrderbookEngine::new(
        event_bus.clone(),
        polymarket_hft::core::constants::ORDERBOOK_STALE_THRESHOLD_MS,
    ));

    // Initialize risk manager
    let risk_manager = Arc::new(RiskManager::new(config.risk.clone(), event_bus.clone()));

    // Get initial balance and initialize risk manager
    match clob_client.get_balance().await {
        Ok(balance) => {
            let usdc_balance = balance.balance.parse::<rust_decimal::Decimal>().unwrap_or_default();
            risk_manager.initialize(usdc_balance);
            info!("USDC Balance: {}", usdc_balance);
        }
        Err(e) => {
            warn!("Failed to get initial balance: {}", e);
            risk_manager.initialize(rust_decimal::Decimal::ZERO);
        }
    }

    // Initialize execution engine
    let execution_engine = Arc::new(ExecutionEngine::new(
        Arc::clone(&clob_client),
        config.trading.clone(),
        event_bus.clone(),
        Arc::clone(&risk_manager),
    ));

    // Initialize arbitrage detector
    let arbitrage_detector = Arc::new(ArbitrageDetector::new(
        config.strategy.clone(),
        Arc::clone(&orderbook_engine),
        event_bus.clone(),
    ));

    // Initialize monitoring
    let metrics_collector = if config.monitoring.prometheus_enabled {
        Some(MetricsCollector::new(config.monitoring.prometheus_port)?)
    } else {
        None
    };

    let health_checker = Arc::new(HealthChecker::new());
    health_checker.register_component("clob_api").await;
    health_checker.register_component("gamma_api").await;
    health_checker.register_component("market_websocket").await;
    health_checker.register_component("user_websocket").await;

    // Initialize WebSocket connections
    let mut market_ws = MarketWebSocket::new(
        &config.api.clob_ws_url,
        event_bus.clone(),
    );

    let mut user_ws = UserWebSocket::new(
        polymarket_hft::core::constants::CLOB_USER_WS_URL,
        signer,
        event_bus.clone(),
    );

    // Start WebSocket connections
    market_ws.start().await?;
    user_ws.start().await?;

    health_checker.update_component("market_websocket", true, None, None).await;
    health_checker.update_component("user_websocket", true, None, None).await;

    info!("Bot initialized successfully");

    // Main trading loop
    let shutdown = Arc::new(RwLock::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    // Spawn shutdown handler
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdown signal received");
                *shutdown_clone.write().await = true;
            }
            Err(e) => {
                error!("Failed to listen for shutdown signal: {}", e);
            }
        }
    });

    // Load initial markets
    info!("Loading markets...");
    match gamma_client.get_markets_filtered(Some(true), Some(false), Some(100), None).await {
        Ok(markets) => {
            let internal_markets: Vec<_> = markets
                .iter()
                .filter_map(|m| m.to_market().ok())
                .collect();

            info!("Loaded {} active markets", internal_markets.len());
            arbitrage_detector.update_markets(internal_markets).await;

            // Subscribe to orderbooks for top markets
            for market in markets.iter().take(20) {
                if let Some(yes_token) = market.yes_token_id() {
                    let _ = market_ws.subscribe(yes_token).await;
                }
                if let Some(no_token) = market.no_token_id() {
                    let _ = market_ws.subscribe(no_token).await;
                }
            }
        }
        Err(e) => {
            error!("Failed to load markets: {}", e);
        }
    }

    health_checker.update_component("clob_api", true, None, None).await;
    health_checker.update_component("gamma_api", true, None, None).await;

    // Trading loop
    let scan_interval = config.strategy.scan_interval;
    let mut interval = tokio::time::interval(scan_interval);

    info!("Starting trading loop (scan interval: {:?})", scan_interval);

    loop {
        interval.tick().await;

        if *shutdown.read().await {
            break;
        }

        // Check if trading is allowed
        if !risk_manager.is_trading_allowed() {
            let status = risk_manager.get_status();
            if status.circuit_breaker_open {
                warn!("Circuit breaker open, skipping scan");
            }
            continue;
        }

        // Scan for opportunities
        let opportunities = arbitrage_detector.scan().await;

        if !opportunities.is_empty() {
            info!("Found {} arbitrage opportunities", opportunities.len());

            // Execute opportunities (best first)
            for opportunity in opportunities.iter().take(3) {
                if config.is_simulation() {
                    info!(
                        "Simulation: Would execute {:?} with expected profit: {}",
                        opportunity.opportunity_type, opportunity.expected_profit
                    );
                } else {
                    match execution_engine.execute_arbitrage(opportunity).await {
                        Ok(result) => {
                            if result.success {
                                info!(
                                    "Arbitrage executed successfully, profit: {}",
                                    result.realized_profit
                                );
                            } else {
                                warn!("Arbitrage execution failed: {:?}", result.error);
                            }
                        }
                        Err(e) => {
                            error!("Arbitrage execution error: {}", e);
                        }
                    }
                }
            }
        }

        // Update metrics
        if let Some(ref metrics) = metrics_collector {
            let stats = execution_engine.get_stats().await;
            metrics.set_realized_profit(stats.total_profit.to_string().parse().unwrap_or(0.0));
            metrics.set_total_fees(stats.total_fees.to_string().parse().unwrap_or(0.0));
            metrics.set_orderbook_count(orderbook_engine.stats().orderbook_count as f64);

            let risk_status = risk_manager.get_status();
            metrics.set_open_positions(risk_status.open_positions as f64);
            metrics.set_total_exposure(risk_status.total_exposure.to_string().parse().unwrap_or(0.0));
            metrics.set_drawdown(risk_status.drawdown_pct.to_string().parse().unwrap_or(0.0));
        }
    }

    // Shutdown
    info!("Shutting down...");
    market_ws.stop().await?;
    user_ws.stop().await?;

    // Cancel any open orders
    if !config.is_simulation() {
        info!("Cancelling open orders...");
        let _ = clob_client.cancel_all_orders(None).await;
    }

    let final_stats = execution_engine.get_stats().await;
    info!("Final statistics:");
    info!("  Total executions: {}", final_stats.total_executions);
    info!("  Successful: {}", final_stats.successful_executions);
    info!("  Failed: {}", final_stats.failed_executions);
    info!("  Net profit: {}", final_stats.net_profit());
    info!("  Success rate: {:.2}%", final_stats.success_rate() * 100.0);

    info!("Shutdown complete");
    Ok(())
}

/// Scan for opportunities without executing
async fn scan_opportunities(config: Config, count: u32, delay: u64) -> Result<()> {
    let event_bus = EventBus::new(1000);

    let gamma_client = GammaClient::new(&config.api)?;
    let orderbook_engine = Arc::new(OrderbookEngine::new(event_bus.clone(), 5000));
    let arbitrage_detector = ArbitrageDetector::new(
        config.strategy.clone(),
        Arc::clone(&orderbook_engine),
        event_bus,
    );

    // Load markets
    let markets = gamma_client.get_markets_filtered(Some(true), Some(false), Some(100), None).await?;
    let internal_markets: Vec<_> = markets
        .iter()
        .filter_map(|m| m.to_market().ok())
        .collect();

    info!("Loaded {} markets", internal_markets.len());
    arbitrage_detector.update_markets(internal_markets).await;

    // Perform scans
    for i in 0..count {
        info!("Scan {}/{}", i + 1, count);

        let opportunities = arbitrage_detector.scan().await;

        if opportunities.is_empty() {
            info!("No opportunities found");
        } else {
            for opp in &opportunities {
                info!(
                    "  {:?}: {} profit ({}%), risk: {:.2}",
                    opp.opportunity_type,
                    opp.expected_profit,
                    opp.expected_profit_pct,
                    opp.risk_score
                );
            }
        }

        if i < count - 1 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }

    Ok(())
}

/// Show market status
async fn show_status(config: Config) -> Result<()> {
    let gamma_client = GammaClient::new(&config.api)?;

    let markets = gamma_client.get_markets_filtered(Some(true), Some(false), Some(20), None).await?;

    println!("\n=== Active Markets ===\n");
    println!("{:<50} {:>10} {:>10} {:>12}", "Market", "Volume", "Liquidity", "Status");
    println!("{}", "-".repeat(85));

    for market in markets {
        println!(
            "{:<50} {:>10.0} {:>10.0} {:>12}",
            &market.question[..market.question.len().min(50)],
            market.volume_num,
            market.liquidity_num,
            if market.active { "Active" } else { "Inactive" }
        );
    }

    Ok(())
}

/// Show account balance
async fn show_balance(config: Config) -> Result<()> {
    let signer = Signer::new(&config.api.private_key)?;
    let clob_client = ClobClient::new(&config.api, signer.clone(), config.blockchain.chain_id)?;

    println!("\n=== Account Balance ===\n");
    println!("Address: {}", signer.address_string());

    let balance = clob_client.get_balance().await?;
    println!("USDC Balance: {}", balance.balance);
    println!("Allowance: {}", balance.allowance);

    let positions = clob_client.get_positions().await?;
    if !positions.is_empty() {
        println!("\n=== Open Positions ===\n");
        println!("{:<40} {:>10} {:>10}", "Token", "Size", "Avg Price");
        println!("{}", "-".repeat(65));

        for pos in positions {
            println!("{:<40} {:>10} {:>10}", pos.token_id, pos.size, pos.avg_price);
        }
    }

    Ok(())
}

/// Redeem winning positions
async fn redeem_positions(config: Config, market_id: Option<String>) -> Result<()> {
    let signer = Signer::new(&config.api.private_key)?;
    let clob_client = ClobClient::new(&config.api, signer, config.blockchain.chain_id)?;

    let positions = clob_client.get_positions().await?;

    if positions.is_empty() {
        println!("No positions to redeem");
        return Ok(());
    }

    // Filter by market if specified
    let to_redeem: Vec<_> = if let Some(ref mid) = market_id {
        positions.into_iter().filter(|p| p.market.contains(mid)).collect()
    } else {
        positions
    };

    if to_redeem.is_empty() {
        println!("No matching positions found");
        return Ok(());
    }

    println!("Found {} positions to check for redemption", to_redeem.len());

    // Note: Actual redemption would require blockchain interaction
    // This is a placeholder for the full implementation
    println!("Redemption requires blockchain interaction - not implemented in this version");

    Ok(())
}
