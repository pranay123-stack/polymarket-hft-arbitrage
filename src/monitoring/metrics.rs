//! Prometheus metrics collection.

use metrics::{counter, gauge, histogram, describe_counter, describe_gauge, describe_histogram, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Metrics collector for the trading bot
pub struct MetricsCollector {
    /// Prometheus handle for getting metrics
    _handle: metrics_exporter_prometheus::PrometheusHandle,
}

impl MetricsCollector {
    /// Create and initialize metrics collector
    pub fn new(port: u16) -> std::io::Result<Self> {
        let builder = PrometheusBuilder::new();
        let handle = builder
            .with_http_listener(([0, 0, 0, 0], port))
            .install_recorder()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        // Register metric descriptions
        Self::register_metrics();

        info!("Metrics server started on port {}", port);

        Ok(Self { _handle: handle })
    }

    /// Register all metric descriptions
    fn register_metrics() {
        // Trading metrics
        describe_counter!("polymarket_orders_submitted_total", Unit::Count, "Total orders submitted");
        describe_counter!("polymarket_orders_filled_total", Unit::Count, "Total orders filled");
        describe_counter!("polymarket_orders_cancelled_total", Unit::Count, "Total orders cancelled");
        describe_counter!("polymarket_orders_rejected_total", Unit::Count, "Total orders rejected");

        describe_counter!("polymarket_trades_total", Unit::Count, "Total trades executed");
        describe_counter!("polymarket_arbitrage_opportunities_total", Unit::Count, "Total arbitrage opportunities detected");
        describe_counter!("polymarket_arbitrage_executed_total", Unit::Count, "Total arbitrage opportunities executed");

        describe_histogram!("polymarket_order_latency_ms", Unit::Milliseconds, "Order submission latency");
        describe_histogram!("polymarket_execution_latency_ms", Unit::Milliseconds, "Total execution latency");
        describe_histogram!("polymarket_orderbook_update_latency_ms", Unit::Milliseconds, "Orderbook update latency");

        // P&L metrics
        describe_gauge!("polymarket_realized_profit_usd", Unit::Count, "Total realized profit in USD");
        describe_gauge!("polymarket_unrealized_pnl_usd", Unit::Count, "Current unrealized P&L in USD");
        describe_gauge!("polymarket_total_fees_usd", Unit::Count, "Total fees paid in USD");

        // Position metrics
        describe_gauge!("polymarket_open_positions", Unit::Count, "Number of open positions");
        describe_gauge!("polymarket_total_exposure_usd", Unit::Count, "Total exposure in USD");

        // Risk metrics
        describe_gauge!("polymarket_drawdown_pct", Unit::Percent, "Current drawdown percentage");
        describe_gauge!("polymarket_daily_pnl_usd", Unit::Count, "Daily P&L in USD");
        describe_counter!("polymarket_circuit_breaker_trips_total", Unit::Count, "Circuit breaker trip count");

        // System metrics
        describe_gauge!("polymarket_websocket_connected", Unit::Count, "WebSocket connection status");
        describe_gauge!("polymarket_orderbook_count", Unit::Count, "Number of tracked orderbooks");
        describe_counter!("polymarket_errors_total", Unit::Count, "Total errors by type");
    }

    // Order metrics
    pub fn record_order_submitted(&self, market_id: &str, side: &str) {
        counter!("polymarket_orders_submitted_total", "market" => market_id.to_string(), "side" => side.to_string()).increment(1);
    }

    pub fn record_order_filled(&self, market_id: &str, side: &str) {
        counter!("polymarket_orders_filled_total", "market" => market_id.to_string(), "side" => side.to_string()).increment(1);
    }

    pub fn record_order_cancelled(&self, market_id: &str) {
        counter!("polymarket_orders_cancelled_total", "market" => market_id.to_string()).increment(1);
    }

    pub fn record_order_rejected(&self, market_id: &str, reason: &str) {
        counter!("polymarket_orders_rejected_total", "market" => market_id.to_string(), "reason" => reason.to_string()).increment(1);
    }

    // Latency metrics
    pub fn record_order_latency(&self, latency_ms: f64) {
        histogram!("polymarket_order_latency_ms").record(latency_ms);
    }

    pub fn record_execution_latency(&self, latency_ms: f64) {
        histogram!("polymarket_execution_latency_ms").record(latency_ms);
    }

    pub fn record_orderbook_update_latency(&self, latency_ms: f64) {
        histogram!("polymarket_orderbook_update_latency_ms").record(latency_ms);
    }

    // Trade metrics
    pub fn record_trade(&self, market_id: &str, side: &str, profit: f64) {
        counter!("polymarket_trades_total", "market" => market_id.to_string(), "side" => side.to_string()).increment(1);
    }

    // Arbitrage metrics
    pub fn record_opportunity_detected(&self, opp_type: &str) {
        counter!("polymarket_arbitrage_opportunities_total", "type" => opp_type.to_string()).increment(1);
    }

    pub fn record_opportunity_executed(&self, opp_type: &str, success: bool) {
        counter!(
            "polymarket_arbitrage_executed_total",
            "type" => opp_type.to_string(),
            "success" => success.to_string()
        ).increment(1);
    }

    // P&L metrics
    pub fn set_realized_profit(&self, profit: f64) {
        gauge!("polymarket_realized_profit_usd").set(profit);
    }

    pub fn set_unrealized_pnl(&self, pnl: f64) {
        gauge!("polymarket_unrealized_pnl_usd").set(pnl);
    }

    pub fn set_total_fees(&self, fees: f64) {
        gauge!("polymarket_total_fees_usd").set(fees);
    }

    // Position metrics
    pub fn set_open_positions(&self, count: f64) {
        gauge!("polymarket_open_positions").set(count);
    }

    pub fn set_total_exposure(&self, exposure: f64) {
        gauge!("polymarket_total_exposure_usd").set(exposure);
    }

    // Risk metrics
    pub fn set_drawdown(&self, pct: f64) {
        gauge!("polymarket_drawdown_pct").set(pct);
    }

    pub fn set_daily_pnl(&self, pnl: f64) {
        gauge!("polymarket_daily_pnl_usd").set(pnl);
    }

    pub fn record_circuit_breaker_trip(&self, reason: &str) {
        counter!("polymarket_circuit_breaker_trips_total", "reason" => reason.to_string()).increment(1);
    }

    // System metrics
    pub fn set_websocket_connected(&self, endpoint: &str, connected: bool) {
        gauge!("polymarket_websocket_connected", "endpoint" => endpoint.to_string()).set(if connected { 1.0 } else { 0.0 });
    }

    pub fn set_orderbook_count(&self, count: f64) {
        gauge!("polymarket_orderbook_count").set(count);
    }

    pub fn record_error(&self, error_type: &str) {
        counter!("polymarket_errors_total", "type" => error_type.to_string()).increment(1);
    }
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector").finish()
    }
}
