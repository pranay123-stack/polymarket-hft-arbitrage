//! Real-time dashboard server for cross-platform arbitrage monitoring.
//!
//! Usage:
//!   cargo run --bin dashboard --features dashboard -- [OPTIONS]
//!
//! Options:
//!   --port <PORT>       HTTP port (default: 8080)
//!   --host <HOST>       Bind address (default: 0.0.0.0)

use std::sync::Arc;
use std::net::SocketAddr;
use axum::{
    Router,
    routing::get,
    extract::{State, Query},
    response::Json,
    http::StatusCode,
};
use tower_http::cors::{CorsLayer, Any};
use tower_http::trace::TraceLayer;
use clap::Parser;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use polymarket_hft::analytics::{
    PnLTracker, StrategyAnalytics, VenueAnalytics,
    DashboardEndpoint, DashboardData, TimeRange,
};

#[derive(Parser, Debug)]
#[command(name = "dashboard")]
#[command(about = "Real-time dashboard server for arbitrage monitoring")]
struct Args {
    /// HTTP port
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
}

/// Application state shared across handlers.
struct AppState {
    dashboard: DashboardEndpoint,
    pnl_tracker: Arc<PnLTracker>,
    strategy_analytics: Arc<StrategyAnalytics>,
    venue_analytics: Arc<VenueAnalytics>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,tower_http=debug")
        .init();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Cross-Platform Arbitrage Dashboard Server v2.0           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // Initialize analytics components
    let pnl_tracker = Arc::new(PnLTracker::new(Decimal::new(100000, 0)));
    let strategy_analytics = Arc::new(StrategyAnalytics::new(10000));
    let venue_analytics = Arc::new(VenueAnalytics::new(10000));

    // Register default strategies and venues
    strategy_analytics.register_strategy("cross_platform", "Cross-Platform Arbitrage").await;
    strategy_analytics.register_strategy("binary_arb", "Binary Mispricing").await;
    strategy_analytics.register_strategy("temporal", "Temporal Arbitrage").await;

    venue_analytics.register_venue("polymarket", "Polymarket").await;
    venue_analytics.register_venue("kalshi", "Kalshi").await;
    venue_analytics.register_venue("opinion", "Opinion").await;

    // Create dashboard endpoint
    let dashboard = DashboardEndpoint::new(
        pnl_tracker.clone(),
        strategy_analytics.clone(),
        venue_analytics.clone(),
    );

    let state = Arc::new(AppState {
        dashboard,
        pnl_tracker,
        strategy_analytics,
        venue_analytics,
    });

    // Build router
    let app = Router::new()
        // Dashboard endpoints
        .route("/api/dashboard", get(get_dashboard))
        .route("/api/pnl", get(get_pnl))
        .route("/api/pnl/series", get(get_pnl_series))
        .route("/api/strategies", get(get_strategies))
        .route("/api/venues", get(get_venues))
        .route("/api/venues/health", get(get_venue_health))
        .route("/api/alerts", get(get_alerts))
        .route("/api/trades/recent", get(get_recent_trades))
        .route("/api/system/status", get(get_system_status))
        // Control endpoints
        .route("/api/trading/enable", get(enable_trading))
        .route("/api/trading/disable", get(disable_trading))
        // Health check
        .route("/health", get(health_check))
        .route("/", get(index))
        // Middleware
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    println!("🚀 Dashboard server starting on http://{}", addr);
    println!();
    println!("Available endpoints:");
    println!("  GET /api/dashboard       - Full dashboard data");
    println!("  GET /api/pnl             - Current P&L snapshot");
    println!("  GET /api/pnl/series      - P&L time series");
    println!("  GET /api/strategies      - Strategy metrics");
    println!("  GET /api/venues          - Venue metrics");
    println!("  GET /api/venues/health   - Venue health status");
    println!("  GET /api/alerts          - Active alerts");
    println!("  GET /api/trades/recent   - Recent trades");
    println!("  GET /api/system/status   - System status");
    println!("  GET /health              - Health check");
    println!();

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Query parameters for time range.
#[derive(Deserialize)]
struct TimeRangeQuery {
    range: Option<String>,
}

impl TimeRangeQuery {
    fn to_time_range(&self) -> TimeRange {
        match self.range.as_deref() {
            Some("1h") => TimeRange::Hour1,
            Some("6h") => TimeRange::Hour6,
            Some("24h") => TimeRange::Hour24,
            Some("7d") => TimeRange::Day7,
            Some("30d") => TimeRange::Day30,
            Some("all") => TimeRange::AllTime,
            _ => TimeRange::Hour24,
        }
    }
}

/// GET /api/dashboard - Full dashboard data
async fn get_dashboard(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TimeRangeQuery>,
) -> Json<DashboardData> {
    let data = state.dashboard.get_dashboard(params.to_time_range()).await;
    Json(data)
}

/// GET /api/pnl - Current P&L snapshot
async fn get_pnl(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let snapshot = state.pnl_tracker.get_snapshot().await;
    Json(serde_json::to_value(snapshot).unwrap())
}

/// GET /api/pnl/series - P&L time series
async fn get_pnl_series(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TimeRangeQuery>,
) -> Json<serde_json::Value> {
    let duration = params.to_time_range().to_duration();
    let series = state.pnl_tracker.get_series("equity", duration).await;
    Json(serde_json::to_value(series).unwrap())
}

/// GET /api/strategies - Strategy metrics
async fn get_strategies(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let strategies = state.dashboard.get_strategies().await;
    Json(serde_json::to_value(strategies).unwrap())
}

/// GET /api/venues - Venue metrics
async fn get_venues(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let venues = state.dashboard.get_venues().await;
    Json(serde_json::to_value(venues).unwrap())
}

/// GET /api/venues/health - Venue health status
async fn get_venue_health(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let health = state.venue_analytics.get_all_health().await;
    Json(serde_json::to_value(health).unwrap())
}

/// GET /api/alerts - Active alerts
async fn get_alerts(
    State(_state): State<Arc<AppState>>,
) -> Json<Vec<serde_json::Value>> {
    // Would integrate with alerting system
    Json(vec![])
}

/// GET /api/trades/recent - Recent trades
async fn get_recent_trades(
    State(_state): State<Arc<AppState>>,
) -> Json<Vec<serde_json::Value>> {
    // Would integrate with trade history
    Json(vec![])
}

/// GET /api/system/status - System status
async fn get_system_status(
    State(state): State<Arc<AppState>>,
) -> Json<SystemStatus> {
    let trading_enabled = state.dashboard.is_trading_enabled().await;
    let venue_health = state.venue_analytics.get_all_health().await;
    let connected = venue_health.iter().filter(|h| h.is_tradeable()).count();

    Json(SystemStatus {
        status: "healthy".to_string(),
        trading_enabled,
        connected_venues: connected as u32,
        uptime_seconds: 0, // Would track actual uptime
        version: "2.0.0".to_string(),
    })
}

#[derive(Serialize)]
struct SystemStatus {
    status: String,
    trading_enabled: bool,
    connected_venues: u32,
    uptime_seconds: u64,
    version: String,
}

/// GET /api/trading/enable - Enable trading
async fn enable_trading(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    state.dashboard.set_trading_enabled(true).await;
    Json(serde_json::json!({"success": true, "trading_enabled": true}))
}

/// GET /api/trading/disable - Disable trading
async fn disable_trading(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    state.dashboard.set_trading_enabled(false).await;
    Json(serde_json::json!({"success": true, "trading_enabled": false}))
}

/// GET /health - Health check
async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// GET / - Index page
async fn index() -> &'static str {
    r#"
Cross-Platform Arbitrage Dashboard API v2.0

Endpoints:
  GET /api/dashboard       - Full dashboard data
  GET /api/pnl             - Current P&L snapshot
  GET /api/pnl/series      - P&L time series
  GET /api/strategies      - Strategy metrics
  GET /api/venues          - Venue metrics
  GET /api/venues/health   - Venue health status
  GET /api/alerts          - Active alerts
  GET /api/trades/recent   - Recent trades
  GET /api/system/status   - System status
  GET /api/trading/enable  - Enable trading
  GET /api/trading/disable - Disable trading
  GET /health              - Health check

Query parameters:
  ?range=1h|6h|24h|7d|30d|all - Time range for data
"#
}
