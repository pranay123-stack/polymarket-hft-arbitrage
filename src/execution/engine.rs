//! Main execution engine for order management.
//!
//! Handles:
//! - Order submission and tracking
//! - Fill monitoring
//! - Position management
//! - Execution analytics

use crate::api::ClobClient;
use crate::core::config::TradingConfig;
use crate::core::error::{Error, Result, TradingError};
use crate::core::events::{Event, EventBus};
use crate::core::types::*;
use crate::execution::AtomicExecutor;
use crate::risk::RiskManager;
use chrono::{Duration, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

/// Main execution engine
pub struct ExecutionEngine {
    clob_client: Arc<ClobClient>,
    config: TradingConfig,
    event_bus: EventBus,
    risk_manager: Arc<RiskManager>,

    // Order tracking
    pending_orders: DashMap<OrderId, PendingOrder>,
    active_orders: DashMap<OrderId, Order>,
    order_fills: DashMap<OrderId, Vec<Trade>>,

    // Execution statistics
    stats: Arc<RwLock<ExecutionStats>>,

    // Atomic executor for arbitrage
    atomic_executor: AtomicExecutor,
}

impl ExecutionEngine {
    /// Create a new execution engine
    pub fn new(
        clob_client: Arc<ClobClient>,
        config: TradingConfig,
        event_bus: EventBus,
        risk_manager: Arc<RiskManager>,
    ) -> Self {
        let atomic_executor = AtomicExecutor::new(
            Arc::clone(&clob_client),
            config.clone(),
            event_bus.clone(),
        );

        Self {
            clob_client,
            config,
            event_bus,
            risk_manager,
            pending_orders: DashMap::new(),
            active_orders: DashMap::new(),
            order_fills: DashMap::new(),
            stats: Arc::new(RwLock::new(ExecutionStats::default())),
            atomic_executor,
        }
    }

    /// Execute an arbitrage opportunity
    #[instrument(skip(self), fields(opportunity_id = %opportunity.id))]
    pub async fn execute_arbitrage(&self, opportunity: &ArbitrageOpportunity) -> Result<ExecutionResult> {
        let start = Instant::now();

        // Validate opportunity is still valid
        if !opportunity.is_valid() {
            return Err(Error::Trading(TradingError::OpportunityExpired));
        }

        // Check risk limits
        self.risk_manager.check_arbitrage(opportunity)?;

        info!(
            "Executing arbitrage opportunity: {:?}, expected profit: {}",
            opportunity.opportunity_type, opportunity.expected_profit
        );

        // Execute based on configuration
        let result = if self.config.atomic_execution {
            self.atomic_executor.execute(opportunity).await
        } else {
            self.execute_sequential(opportunity).await
        };

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.total_executions += 1;
            stats.total_latency_ms += start.elapsed().as_millis() as u64;

            match &result {
                Ok(exec_result) => {
                    if exec_result.success {
                        stats.successful_executions += 1;
                        stats.total_profit += exec_result.realized_profit;
                    } else {
                        stats.failed_executions += 1;
                    }
                }
                Err(_) => {
                    stats.failed_executions += 1;
                }
            }
        }

        result
    }

    /// Execute legs sequentially
    async fn execute_sequential(&self, opportunity: &ArbitrageOpportunity) -> Result<ExecutionResult> {
        let mut trades = Vec::new();
        let mut total_cost = Decimal::ZERO;
        let mut total_fees = Decimal::ZERO;

        // Sort legs by priority
        let mut legs = opportunity.legs.clone();
        legs.sort_by_key(|l| l.priority);

        for leg in &legs {
            let order = leg.to_order();

            // Submit order
            let order_result = self.submit_order(&order).await?;

            // Wait for fill with timeout
            let fill = timeout(
                self.config.order_timeout,
                self.wait_for_fill(&order_result.id),
            )
            .await
            .map_err(|_| Error::Trading(TradingError::OrderExpired {
                order_id: order_result.id.clone(),
            }))??;

            // Update tracking
            total_cost += fill.price.0 * fill.quantity.0;
            total_fees += fill.fee;
            trades.push(fill);
        }

        // Calculate realized profit
        let realized_profit = opportunity.expected_profit - total_fees;

        Ok(ExecutionResult {
            opportunity_id: opportunity.id,
            success: true,
            trades,
            realized_profit,
            total_cost,
            total_fees,
            execution_time_ms: 0, // Will be set by caller
            error: None,
        })
    }

    /// Submit a single order
    #[instrument(skip(self), fields(order_id = %order.id))]
    pub async fn submit_order(&self, order: &Order) -> Result<Order> {
        // Pre-flight risk check
        self.risk_manager.check_order(order)?;

        // Record pending order
        self.pending_orders.insert(
            order.id.clone(),
            PendingOrder {
                order: order.clone(),
                submitted_at: Utc::now(),
                status: PendingOrderStatus::Submitting,
            },
        );

        // Submit to CLOB
        let start = Instant::now();
        let response = self.clob_client.place_order(order).await?;
        let latency = start.elapsed().as_millis() as u64;

        info!(
            "Order submitted: {} in {}ms",
            response.id, latency
        );

        // Update tracking
        self.pending_orders.remove(&order.id);
        let submitted_order = Order {
            id: response.id.clone(),
            ..order.clone()
        };
        self.active_orders.insert(response.id.clone(), submitted_order.clone());

        // Emit event
        self.event_bus.publish(Event::order(submitted_order.clone(), OrderStatus::Live));

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.orders_submitted += 1;
            stats.total_order_latency_ms += latency;
        }

        Ok(submitted_order)
    }

    /// Cancel an order
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let response = self.clob_client.cancel_order(order_id).await?;

        if response.canceled.contains(&order_id.to_string()) {
            self.active_orders.remove(order_id);
            info!("Order cancelled: {}", order_id);

            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.orders_cancelled += 1;
            }
        } else {
            warn!("Order not cancelled: {}", order_id);
        }

        Ok(())
    }

    /// Cancel all orders for a market
    pub async fn cancel_all(&self, market_id: Option<&str>) -> Result<usize> {
        let response = self.clob_client.cancel_all_orders(market_id).await?;
        let count = response.canceled.len();

        for order_id in &response.canceled {
            self.active_orders.remove(order_id);
        }

        info!("Cancelled {} orders", count);
        Ok(count)
    }

    /// Wait for an order to be filled
    async fn wait_for_fill(&self, order_id: &str) -> Result<Trade> {
        // In production, this would listen to WebSocket events
        // For now, we'll poll the order status
        let poll_interval = std::time::Duration::from_millis(100);
        let max_polls = (self.config.order_timeout.as_millis() / 100) as u32;

        for _ in 0..max_polls {
            // Check if we have fills recorded
            if let Some(fills) = self.order_fills.get(order_id) {
                if !fills.is_empty() {
                    return Ok(fills.first().unwrap().clone());
                }
            }

            // Poll order status
            if let Ok(order_response) = self.clob_client.get_order(order_id).await {
                match order_response.status.to_uppercase().as_str() {
                    "MATCHED" | "FILLED" => {
                        // Create trade from order response
                        return Ok(self.create_trade_from_fill(&order_response)?);
                    }
                    "CANCELLED" | "CANCELED" => {
                        return Err(Error::Trading(TradingError::OrderCancelled {
                            order_id: order_id.to_string(),
                        }));
                    }
                    "REJECTED" => {
                        return Err(Error::Trading(TradingError::ExecutionFailed {
                            order_id: order_id.to_string(),
                            reason: "Order rejected".to_string(),
                        }));
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(poll_interval).await;
        }

        Err(Error::Trading(TradingError::OrderExpired {
            order_id: order_id.to_string(),
        }))
    }

    /// Create a trade from order fill response
    fn create_trade_from_fill(&self, response: &crate::api::clob::OrderResponse) -> Result<Trade> {
        let price = response.price.parse::<Decimal>()
            .map_err(|e| Error::Parse(format!("Invalid price: {}", e)))?;
        let quantity = response.original_size.parse::<Decimal>()
            .map_err(|e| Error::Parse(format!("Invalid size: {}", e)))?;

        let side = match response.side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return Err(Error::Parse(format!("Invalid side: {}", response.side))),
        };

        // Estimate fee
        let fee = price * quantity * self.config.fee_rate;

        Ok(Trade {
            id: Uuid::new_v4().to_string(),
            order_id: response.id.clone(),
            market_id: response.market.clone().unwrap_or_default(),
            token_id: response.token_id.clone(),
            side,
            outcome: Outcome::Yes, // Will be determined by context
            price: Price::new_unchecked(price),
            quantity: Quantity(quantity),
            fee,
            timestamp: Utc::now(),
            tx_hash: None,
        })
    }

    /// Record a fill from WebSocket event
    pub fn record_fill(&self, trade: Trade) {
        // Store in fills map
        self.order_fills
            .entry(trade.order_id.clone())
            .or_insert_with(Vec::new)
            .push(trade.clone());

        // Emit event
        self.event_bus.publish(Event::trade(trade));
    }

    /// Get all active orders
    pub fn get_active_orders(&self) -> Vec<Order> {
        self.active_orders.iter().map(|r| r.value().clone()).collect()
    }

    /// Get execution statistics
    pub async fn get_stats(&self) -> ExecutionStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = ExecutionStats::default();
    }
}

/// Pending order tracking
#[derive(Debug, Clone)]
struct PendingOrder {
    order: Order,
    submitted_at: chrono::DateTime<Utc>,
    status: PendingOrderStatus,
}

#[derive(Debug, Clone)]
enum PendingOrderStatus {
    Submitting,
    Submitted,
    Failed,
}

/// Result of executing an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub opportunity_id: Uuid,
    pub success: bool,
    pub trades: Vec<Trade>,
    pub realized_profit: Decimal,
    pub total_cost: Decimal,
    pub total_fees: Decimal,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}

impl ExecutionResult {
    /// Create a failed result
    pub fn failed(opportunity_id: Uuid, error: String) -> Self {
        Self {
            opportunity_id,
            success: false,
            trades: Vec::new(),
            realized_profit: Decimal::ZERO,
            total_cost: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            execution_time_ms: 0,
            error: Some(error),
        }
    }
}

/// Execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_cancelled: u64,
    pub orders_rejected: u64,
    pub total_profit: Decimal,
    pub total_loss: Decimal,
    pub total_fees: Decimal,
    pub total_latency_ms: u64,
    pub total_order_latency_ms: u64,
}

impl ExecutionStats {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_executions == 0 {
            0.0
        } else {
            self.successful_executions as f64 / self.total_executions as f64
        }
    }

    /// Calculate fill rate
    pub fn fill_rate(&self) -> f64 {
        if self.orders_submitted == 0 {
            0.0
        } else {
            self.orders_filled as f64 / self.orders_submitted as f64
        }
    }

    /// Calculate average execution latency
    pub fn avg_execution_latency_ms(&self) -> f64 {
        if self.total_executions == 0 {
            0.0
        } else {
            self.total_latency_ms as f64 / self.total_executions as f64
        }
    }

    /// Calculate average order latency
    pub fn avg_order_latency_ms(&self) -> f64 {
        if self.orders_submitted == 0 {
            0.0
        } else {
            self.total_order_latency_ms as f64 / self.orders_submitted as f64
        }
    }

    /// Net profit
    pub fn net_profit(&self) -> Decimal {
        self.total_profit - self.total_loss - self.total_fees
    }
}

impl std::fmt::Debug for ExecutionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionEngine")
            .field("active_orders", &self.active_orders.len())
            .field("pending_orders", &self.pending_orders.len())
            .finish()
    }
}
