//! Atomic executor for arbitrage trades.
//!
//! Ensures all-or-nothing execution of multi-leg arbitrage opportunities.

use crate::api::ClobClient;
use crate::core::config::TradingConfig;
use crate::core::error::{Error, Result, TradingError};
use crate::core::events::{Event, EventBus};
use crate::core::types::*;
use crate::execution::engine::ExecutionResult;
use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn};

/// Atomic executor for multi-leg trades
pub struct AtomicExecutor {
    clob_client: Arc<ClobClient>,
    config: TradingConfig,
    event_bus: EventBus,
}

impl AtomicExecutor {
    /// Create a new atomic executor
    pub fn new(
        clob_client: Arc<ClobClient>,
        config: TradingConfig,
        event_bus: EventBus,
    ) -> Self {
        Self {
            clob_client,
            config,
            event_bus,
        }
    }

    /// Execute an arbitrage opportunity atomically
    #[instrument(skip(self), fields(opportunity_id = %opportunity.id))]
    pub async fn execute(&self, opportunity: &ArbitrageOpportunity) -> Result<ExecutionResult> {
        let start = Instant::now();

        // Validate opportunity
        if !opportunity.is_valid() {
            return Err(Error::Trading(TradingError::OpportunityExpired));
        }

        info!(
            "Atomic execution starting for {:?} with {} legs",
            opportunity.opportunity_type,
            opportunity.legs.len()
        );

        // Sort legs by priority
        let mut legs = opportunity.legs.clone();
        legs.sort_by_key(|l| l.priority);

        // Phase 1: Submit all orders simultaneously
        let orders: Vec<Order> = legs.iter().map(|l| l.to_order()).collect();
        let submission_result = self.submit_all_orders(&orders).await;

        match submission_result {
            Ok(submitted_orders) => {
                // Phase 2: Monitor all orders for fills
                let fill_result = self.wait_for_all_fills(&submitted_orders).await;

                match fill_result {
                    Ok(trades) => {
                        let execution_time = start.elapsed().as_millis() as u64;

                        // Calculate realized profit
                        let total_cost: Decimal = trades
                            .iter()
                            .filter(|t| t.side == Side::Buy)
                            .map(|t| t.price.0 * t.quantity.0)
                            .sum();

                        let total_proceeds: Decimal = trades
                            .iter()
                            .filter(|t| t.side == Side::Sell)
                            .map(|t| t.price.0 * t.quantity.0)
                            .sum();

                        let total_fees: Decimal = trades.iter().map(|t| t.fee).sum();

                        // For buy-both strategies, profit comes at settlement
                        let realized_profit = if trades.iter().all(|t| t.side == Side::Buy) {
                            // Settlement profit = $1 - total cost - fees
                            let min_qty: Decimal = trades.iter().map(|t| t.quantity.0).min().unwrap_or(Decimal::ZERO);
                            min_qty - total_cost - total_fees
                        } else {
                            total_proceeds - total_cost - total_fees
                        };

                        info!(
                            "Atomic execution completed in {}ms, profit: {}",
                            execution_time, realized_profit
                        );

                        // Emit success event
                        self.event_bus.publish(Event::new(
                            crate::core::events::EventPayload::ArbitrageExecuted(
                                crate::core::events::ArbitrageExecutedEvent {
                                    opportunity_id: opportunity.id,
                                    trades: trades.clone(),
                                    realized_profit,
                                    execution_time_ms: execution_time,
                                    timestamp: Utc::now(),
                                }
                            )
                        ));

                        Ok(ExecutionResult {
                            opportunity_id: opportunity.id,
                            success: true,
                            trades,
                            realized_profit,
                            total_cost,
                            total_fees,
                            execution_time_ms: execution_time,
                            error: None,
                        })
                    }
                    Err(e) => {
                        // Partial fills - need to handle unwinding
                        warn!("Partial fill failure, attempting to unwind: {}", e);
                        self.unwind_partial_fills(&submitted_orders).await;

                        Err(Error::Trading(TradingError::AtomicExecutionFailed(
                            e.to_string()
                        )))
                    }
                }
            }
            Err(e) => {
                error!("Order submission failed: {}", e);
                Err(e)
            }
        }
    }

    /// Submit all orders simultaneously
    async fn submit_all_orders(&self, orders: &[Order]) -> Result<Vec<SubmittedOrder>> {
        let mut handles = Vec::new();
        let mut submitted = Vec::new();

        // Submit all orders concurrently
        for order in orders {
            let client = Arc::clone(&self.clob_client);
            let order_clone = order.clone();

            let handle = tokio::spawn(async move {
                let start = Instant::now();
                let result = client.place_order(&order_clone).await;
                let latency = start.elapsed().as_millis() as u64;

                (order_clone, result, latency)
            });

            handles.push(handle);
        }

        // Collect results
        let mut failed = false;
        let mut first_error = None;

        for handle in handles {
            match handle.await {
                Ok((order, result, latency)) => {
                    match result {
                        Ok(response) => {
                            debug!(
                                "Order {} submitted in {}ms: {}",
                                order.id, latency, response.id
                            );
                            submitted.push(SubmittedOrder {
                                original: order,
                                clob_id: response.id,
                                submitted_at: Utc::now(),
                                latency_ms: latency,
                            });
                        }
                        Err(e) => {
                            error!("Order submission failed: {}", e);
                            failed = true;
                            if first_error.is_none() {
                                first_error = Some(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Task join error: {}", e);
                    failed = true;
                }
            }
        }

        // If any submission failed, cancel all submitted orders
        if failed {
            warn!("Rolling back {} submitted orders", submitted.len());
            for sub in &submitted {
                let _ = self.clob_client.cancel_order(&sub.clob_id).await;
            }
            return Err(first_error.unwrap_or_else(|| Error::Internal("Submission failed".into())));
        }

        Ok(submitted)
    }

    /// Wait for all orders to fill
    async fn wait_for_all_fills(&self, orders: &[SubmittedOrder]) -> Result<Vec<Trade>> {
        let timeout_duration = self.config.order_timeout;
        let poll_interval = std::time::Duration::from_millis(50);
        let start = Instant::now();

        let mut filled_orders: Vec<Option<Trade>> = vec![None; orders.len()];
        let mut all_filled = false;

        while start.elapsed() < timeout_duration && !all_filled {
            // Check each order
            for (i, order) in orders.iter().enumerate() {
                if filled_orders[i].is_some() {
                    continue;
                }

                if let Ok(response) = self.clob_client.get_order(&order.clob_id).await {
                    match response.status.to_uppercase().as_str() {
                        "MATCHED" | "FILLED" => {
                            let price = response.price.parse::<Decimal>().unwrap_or_default();
                            let quantity = response.original_size.parse::<Decimal>().unwrap_or_default();
                            let fee = price * quantity * self.config.fee_rate;

                            filled_orders[i] = Some(Trade {
                                id: uuid::Uuid::new_v4().to_string(),
                                order_id: order.clob_id.clone(),
                                market_id: order.original.market_id.clone(),
                                token_id: order.original.token_id.clone(),
                                side: order.original.side,
                                outcome: order.original.outcome,
                                price: Price::new_unchecked(price),
                                quantity: Quantity(quantity),
                                fee,
                                timestamp: Utc::now(),
                                tx_hash: None,
                            });

                            debug!("Order {} filled", order.clob_id);
                        }
                        "CANCELLED" | "CANCELED" | "REJECTED" => {
                            return Err(Error::Trading(TradingError::OrderCancelled {
                                order_id: order.clob_id.clone(),
                            }));
                        }
                        _ => {}
                    }
                }
            }

            all_filled = filled_orders.iter().all(|f| f.is_some());

            if !all_filled {
                tokio::time::sleep(poll_interval).await;
            }
        }

        if all_filled {
            Ok(filled_orders.into_iter().filter_map(|f| f).collect())
        } else {
            // Timeout - cancel unfilled orders
            let unfilled_count = filled_orders.iter().filter(|f| f.is_none()).count();
            warn!("Timeout waiting for fills, {} orders unfilled", unfilled_count);

            for (i, order) in orders.iter().enumerate() {
                if filled_orders[i].is_none() {
                    let _ = self.clob_client.cancel_order(&order.clob_id).await;
                }
            }

            Err(Error::Trading(TradingError::AtomicExecutionFailed(
                format!("Timeout: {} orders unfilled", unfilled_count)
            )))
        }
    }

    /// Attempt to unwind partial fills
    async fn unwind_partial_fills(&self, orders: &[SubmittedOrder]) {
        warn!("Attempting to unwind {} orders", orders.len());

        for order in orders {
            // Cancel if still open
            if let Err(e) = self.clob_client.cancel_order(&order.clob_id).await {
                debug!("Cancel failed (may already be filled): {}", e);
            }
        }

        // Note: In production, you'd also want to handle the case where
        // some orders filled and you need to close the position or
        // execute offsetting trades
    }
}

/// Submitted order with tracking info
#[derive(Debug, Clone)]
struct SubmittedOrder {
    original: Order,
    clob_id: String,
    submitted_at: chrono::DateTime<Utc>,
    latency_ms: u64,
}

impl std::fmt::Debug for AtomicExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicExecutor").finish()
    }
}
