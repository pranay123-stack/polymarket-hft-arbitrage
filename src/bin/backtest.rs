//! Backtest runner for cross-platform arbitrage strategies.
//!
//! Usage:
//!   cargo run --bin backtest --features backtesting -- [OPTIONS]
//!
//! Options:
//!   --data <PATH>       Path to historical tick data (CSV or JSON)
//!   --start <DATE>      Start date (YYYY-MM-DD)
//!   --end <DATE>        End date (YYYY-MM-DD)
//!   --capital <AMOUNT>  Initial capital (default: 100000)
//!   --config <PATH>     Strategy configuration file
//!   --output <PATH>     Output report path
//!   --conservative      Use conservative fill/slippage models
//!   --stress-test       Run with stress test latency profile

use std::path::PathBuf;
use clap::{Parser, ValueEnum};
use rust_decimal::Decimal;
use chrono::NaiveDate;
use tokio::fs;

use polymarket_hft::simulation::{
    SimulatedExecutor, SimulatorConfig, BacktestResult,
    market_replay::{ReplayConfig, HistoricalTick, TickBuilder},
    latency_model::{LatencyProfile, Venue},
};

#[derive(Parser, Debug)]
#[command(name = "backtest")]
#[command(about = "Run backtests for cross-platform arbitrage strategies")]
struct Args {
    /// Path to historical tick data
    #[arg(short, long)]
    data: Option<PathBuf>,

    /// Start date (YYYY-MM-DD)
    #[arg(long)]
    start: Option<String>,

    /// End date (YYYY-MM-DD)
    #[arg(long)]
    end: Option<String>,

    /// Initial capital
    #[arg(short, long, default_value = "100000")]
    capital: f64,

    /// Minimum profit threshold in basis points
    #[arg(long, default_value = "5")]
    min_profit_bps: f64,

    /// Strategy configuration file
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,

    /// Output report path
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Use conservative fill/slippage models
    #[arg(long)]
    conservative: bool,

    /// Run with stress test latency profile
    #[arg(long)]
    stress_test: bool,

    /// Verbosity level
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Run with sample data for demonstration
    #[arg(long)]
    demo: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .init();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Cross-Platform Arbitrage Backtest Runner v2.0            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // Build configuration
    let latency_profile = if args.stress_test {
        println!("⚠️  Running in STRESS TEST mode (5x latency, high variance)");
        LatencyProfile::StressTest
    } else if args.conservative {
        println!("📊 Running in CONSERVATIVE mode");
        LatencyProfile::HighVolatility
    } else {
        LatencyProfile::Normal
    };

    let capital = Decimal::from_f64_retain(args.capital)
        .unwrap_or(Decimal::new(100000, 0));
    let min_profit_bps = Decimal::from_f64_retain(args.min_profit_bps)
        .unwrap_or(Decimal::new(5, 0));

    println!("Configuration:");
    println!("  Initial Capital: ${}", capital);
    println!("  Min Profit Threshold: {} bps", min_profit_bps);
    println!("  Conservative Mode: {}", args.conservative);
    println!("  Latency Profile: {:?}", latency_profile);
    println!();

    // Load or generate tick data
    let ticks = if args.demo {
        println!("📈 Generating demo tick data...");
        generate_demo_ticks()
    } else if let Some(data_path) = &args.data {
        println!("📂 Loading tick data from {:?}...", data_path);
        load_tick_data(data_path).await?
    } else {
        println!("⚠️  No data file specified. Use --data <PATH> or --demo");
        println!("    Running with minimal demo data...");
        generate_demo_ticks()
    };

    println!("  Loaded {} ticks", ticks.len());
    println!();

    // Build simulator config
    let config = SimulatorConfig {
        initial_capital: capital,
        min_profit_threshold_bps: min_profit_bps,
        conservative_mode: args.conservative,
        latency_profile,
        verbose_logging: args.verbose > 0,
        ..Default::default()
    };

    // Run backtest
    println!("🚀 Running backtest...");
    println!();

    let mut executor = SimulatedExecutor::new(config);
    let result = executor.run_backtest(ticks).await?;

    // Display results
    print_results(&result);

    // Save report if output path specified
    if let Some(output_path) = &args.output {
        save_report(&result, output_path).await?;
        println!("\n📄 Report saved to {:?}", output_path);
    }

    Ok(())
}

/// Generate demo tick data for testing.
fn generate_demo_ticks() -> Vec<HistoricalTick> {
    use rust_decimal_macros::dec;

    let mut builder = TickBuilder::new(
        chrono::Utc::now().timestamp_millis() as u64 - 3600000 // 1 hour ago
    );

    // Generate realistic cross-venue arbitrage scenarios
    for i in 0..100 {
        let base_price = dec!(0.50) + Decimal::from(i % 10) * dec!(0.01);

        // Kalshi quote (tighter spread, faster)
        builder.quote(
            Venue::Kalshi,
            "BTC-100K-DEC",
            base_price - dec!(0.01),
            Decimal::new(1000, 0),
            base_price + dec!(0.01),
            Decimal::new(1000, 0),
        );

        builder.advance(50);

        // Polymarket quote (wider spread, may create arb)
        let poly_offset = if i % 5 == 0 { dec!(0.03) } else { dec!(0.01) };
        builder.quote(
            Venue::Polymarket,
            "BTC-100K-DEC",
            base_price - poly_offset - dec!(0.02),
            Decimal::new(500, 0),
            base_price + poly_offset + dec!(0.02),
            Decimal::new(500, 0),
        );

        builder.advance(50);

        // Opinion quote
        builder.quote(
            Venue::Opinion,
            "BTC-100K-DEC",
            base_price - dec!(0.015),
            Decimal::new(800, 0),
            base_price + dec!(0.015),
            Decimal::new(800, 0),
        );

        builder.advance(100);
    }

    builder.build()
}

/// Load tick data from file.
async fn load_tick_data(path: &PathBuf) -> Result<Vec<HistoricalTick>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path).await?;

    if path.extension().map(|e| e == "json").unwrap_or(false) {
        // JSON format
        let ticks: Vec<HistoricalTick> = serde_json::from_str(&content)?;
        Ok(ticks)
    } else {
        // Assume CSV - would need proper parsing
        // For now, return empty and let replay engine handle CSV
        println!("  CSV loading via replay engine...");
        Ok(Vec::new())
    }
}

/// Print backtest results.
fn print_results(result: &BacktestResult) {
    let m = &result.metrics;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    BACKTEST RESULTS                          ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ P&L Summary                                                  ║");
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║   Gross P&L:        {:>15}                         ║", format!("${:.2}", m.gross_pnl));
    println!("║   Net P&L:          {:>15}                         ║", format!("${:.2}", m.net_pnl));
    println!("║   Total Fees:       {:>15}                         ║", format!("${:.2}", m.total_fees));
    println!("║   Return:           {:>15}                         ║", format!("{:.2}%", m.return_pct));
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║ Trade Statistics                                             ║");
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║   Total Trades:     {:>15}                         ║", m.total_trades);
    println!("║   Winning:          {:>15}                         ║", m.winning_trades);
    println!("║   Losing:           {:>15}                         ║", m.losing_trades);
    println!("║   Win Rate:         {:>15}                         ║", format!("{:.1}%", m.win_rate * 100.0));
    println!("║   Profit Factor:    {:>15}                         ║", format!("{:.2}", m.profit_factor));
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║ Risk Metrics                                                 ║");
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║   Sharpe Ratio:     {:>15}                         ║", format!("{:.2}", m.sharpe_ratio));
    println!("║   Sortino Ratio:    {:>15}                         ║", format!("{:.2}", m.sortino_ratio));
    println!("║   Max Drawdown:     {:>15}                         ║", format!("${:.2}", m.max_drawdown));
    println!("║   Max DD %:         {:>15}                         ║", format!("{:.2}%", m.max_drawdown_pct));
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║ Execution Quality                                            ║");
    println!("╟──────────────────────────────────────────────────────────────╢");
    println!("║   Avg Latency:      {:>15}                         ║", format!("{:.1}ms", m.avg_latency_ms));
    println!("║   Avg Slippage:     {:>15}                         ║", format!("{:.2}bps", m.avg_slippage_bps));
    println!("║   Fill Rate:        {:>15}                         ║", format!("{:.1}%", m.fill_rate * 100.0));
    println!("╚══════════════════════════════════════════════════════════════╝");

    // Per-venue breakdown
    if !result.venue_metrics.is_empty() {
        println!();
        println!("Per-Venue Breakdown:");
        for (venue, vm) in &result.venue_metrics {
            println!("  {:?}: {} trades, ${:.2} volume, {:.1}ms avg latency",
                venue, vm.total_trades, vm.total_volume, vm.avg_latency_ms);
        }
    }
}

/// Save report to file.
async fn save_report(result: &BacktestResult, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let report = serde_json::to_string_pretty(result)?;
    fs::write(path, report).await?;
    Ok(())
}
