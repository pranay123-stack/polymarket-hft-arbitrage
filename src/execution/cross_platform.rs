//! Cross-Platform Execution Engine
//!
//! This module handles the critical challenge of executing arbitrage across
//! multiple venues (Polymarket ↔ Kalshi/Opinion) where fills are NOT atomic.
//!
//! ## Key Challenges Addressed:
//! 1. **Non-atomic fills**: Neither leg can be guaranteed to fill
//! 2. **Execution sequencing**: Which leg to execute first based on liquidity/latency
//! 3. **Partial fill handling**: First leg fills but hedge side moves or partially fills
//! 4. **State recovery**: Handling failures and incomplete states across venues
//!
//! ## Execution Strategy:
//! - Analyze liquidity on both venues before execution
//! - Execute on the MORE liquid venue first (higher fill probability)
//! - Use aggressive pricing on hedge leg to maximize fill probability
//! - Implement configurable abort thresholds for partial fills
//! - Track state persistently for crash recovery

use crate::api::{
    ClobClient,
    kalshi::{KalshiClient, KalshiOrder},
    opinion::{OpinionClient, OpinionOrder, OpinionAction},
};
use crate::core::{
    error::{ArbitrageError, Result},
    types::{
        ArbitrageLeg, ArbitrageOpportunity, Order, OrderStatus, OrderType,
        Outcome, Price, Quantity, Side, TokenId, Trade,
    },
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Supported trading venues
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Polymarket,
    Kalshi,
    Opinion,
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Venue::Polymarket => write!(f, "Polymarket"),
            Venue::Kalshi => write!(f, "Kalshi"),
            Venue::Opinion => write!(f, "Opinion"),
        }
    }
}

/// Cross-platform execution configuration
#[derive(Debug, Clone)]
pub struct CrossPlatformConfig {
    /// Maximum time to wait for first leg fill (ms)
    pub first_leg_timeout_ms: u64,
    /// Maximum time to wait for hedge leg fill (ms)
    pub hedge_leg_timeout_ms: u64,
    /// Maximum slippage on hedge leg before aborting (e.g., 0.02 = 2%)
    pub max_hedge_slippage: Decimal,
    /// Minimum fill percentage on first leg before hedging (e.g., 0.5 = 50%)
    pub min_first_leg_fill_pct: Decimal,
    /// Aggressiveness factor for hedge pricing (higher = more aggressive, more likely to fill)
    pub hedge_price_aggression: Decimal,
    /// Whether to use IOC (immediate-or-cancel) for hedge leg
    pub use_ioc_for_hedge: bool,
    /// Maximum retry attempts for hedge leg
    pub max_hedge_retries: u32,
    /// Delay between hedge retries (ms)
    pub hedge_retry_delay_ms: u64,
    /// Abort threshold: if hedge moves more than this after first leg fills, abort and unwind
    pub abort_price_threshold: Decimal,
    /// Enable persistent state recovery
    pub enable_state_recovery: bool,
}

impl Default for CrossPlatformConfig {
    fn default() -> Self {
        Self {
            first_leg_timeout_ms: 3000,
            hedge_leg_timeout_ms: 5000,
            max_hedge_slippage: dec!(0.03),      // 3% max slippage
            min_first_leg_fill_pct: dec!(0.5),   // Need at least 50% fill
            hedge_price_aggression: dec!(0.01),  // Add 1 cent to hedge price
            use_ioc_for_hedge: true,
            max_hedge_retries: 3,
            hedge_retry_delay_ms: 100,
            abort_price_threshold: dec!(0.05),   // 5% move triggers abort
            enable_state_recovery: true,
        }
    }
}

/// State of a cross-platform execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionState {
    /// Initial state, not started
    Pending,
    /// First leg order submitted
    FirstLegSubmitted {
        venue: Venue,
        order_id: String,
        submitted_at: DateTime<Utc>,
    },
    /// First leg partially filled, waiting
    FirstLegPartialFill {
        venue: Venue,
        order_id: String,
        filled_qty: Decimal,
        remaining_qty: Decimal,
        avg_price: Decimal,
    },
    /// First leg fully filled, hedge leg pending
    FirstLegFilled {
        venue: Venue,
        order_id: String,
        filled_qty: Decimal,
        avg_price: Decimal,
        filled_at: DateTime<Utc>,
    },
    /// Hedge leg submitted
    HedgeLegSubmitted {
        first_leg: FilledLegInfo,
        hedge_venue: Venue,
        hedge_order_id: String,
        submitted_at: DateTime<Utc>,
    },
    /// Hedge leg partially filled
    HedgeLegPartialFill {
        first_leg: FilledLegInfo,
        hedge_venue: Venue,
        hedge_order_id: String,
        hedge_filled_qty: Decimal,
        hedge_remaining_qty: Decimal,
        hedge_avg_price: Decimal,
    },
    /// Both legs fully filled - SUCCESS
    Completed {
        first_leg: FilledLegInfo,
        hedge_leg: FilledLegInfo,
        realized_profit: Decimal,
        completed_at: DateTime<Utc>,
    },
    /// Execution aborted due to conditions
    Aborted {
        reason: AbortReason,
        first_leg: Option<FilledLegInfo>,
        hedge_leg: Option<FilledLegInfo>,
        aborted_at: DateTime<Utc>,
    },
    /// Unwinding position due to failed hedge
    Unwinding {
        first_leg: FilledLegInfo,
        partial_hedge: Option<FilledLegInfo>,
        unwind_reason: String,
    },
    /// Unwind completed
    UnwindCompleted {
        first_leg: FilledLegInfo,
        partial_hedge: Option<FilledLegInfo>,
        unwind_trades: Vec<FilledLegInfo>,
        net_pnl: Decimal,
        completed_at: DateTime<Utc>,
    },
    /// Failed state requiring manual intervention
    Failed {
        error: String,
        first_leg: Option<FilledLegInfo>,
        hedge_leg: Option<FilledLegInfo>,
        failed_at: DateTime<Utc>,
    },
}

/// Information about a filled leg
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilledLegInfo {
    pub venue: Venue,
    pub order_id: String,
    pub market_id: String,
    pub token_id: String,
    pub side: Side,
    pub outcome: Outcome,
    pub filled_qty: Decimal,
    pub avg_price: Decimal,
    pub fees: Decimal,
    pub filled_at: DateTime<Utc>,
}

/// Reason for aborting execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AbortReason {
    /// First leg didn't fill in time
    FirstLegTimeout,
    /// First leg fill percentage too low
    InsufficientFirstLegFill { fill_pct: Decimal },
    /// Hedge price moved beyond threshold
    HedgePriceSlippage { expected: Decimal, actual: Decimal },
    /// Hedge leg didn't fill after retries
    HedgeLegTimeout,
    /// Manual abort triggered
    ManualAbort,
    /// Risk limit triggered
    RiskLimitTriggered { reason: String },
    /// Venue connectivity issue
    VenueDisconnected { venue: Venue },
}

/// A cross-platform arbitrage leg specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossPlatformLeg {
    pub venue: Venue,
    pub market_id: String,
    pub token_id: String,
    pub side: Side,
    pub outcome: Outcome,
    pub target_price: Price,
    pub target_qty: Quantity,
    pub is_hedge: bool,
    /// Estimated latency to this venue (ms)
    pub estimated_latency_ms: u64,
    /// Available liquidity at target price
    pub available_liquidity: Decimal,
}

impl CrossPlatformLeg {
    /// Calculate liquidity score (higher = more liquid)
    pub fn liquidity_score(&self) -> Decimal {
        // Combine available liquidity with latency consideration
        // Lower latency and higher liquidity = higher score
        let latency_factor = Decimal::from(1000) / Decimal::from(self.estimated_latency_ms.max(1));
        self.available_liquidity * latency_factor
    }
}

/// Cross-platform arbitrage opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossPlatformOpportunity {
    pub id: Uuid,
    pub leg_a: CrossPlatformLeg,
    pub leg_b: CrossPlatformLeg,
    pub expected_profit: Decimal,
    pub expected_profit_pct: Decimal,
    pub required_capital: Decimal,
    pub detected_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// Market correlation score (0-1, higher = more correlated = safer)
    pub correlation_score: f64,
}

impl CrossPlatformOpportunity {
    /// Determine which leg should be executed first
    ///
    /// Strategy: Execute on the MORE liquid venue first because:
    /// 1. Higher probability of getting filled
    /// 2. Less market impact
    /// 3. More time to adjust hedge if needed
    pub fn first_leg(&self) -> &CrossPlatformLeg {
        if self.leg_a.liquidity_score() >= self.leg_b.liquidity_score() {
            &self.leg_a
        } else {
            &self.leg_b
        }
    }

    /// Get the hedge leg
    pub fn hedge_leg(&self) -> &CrossPlatformLeg {
        if self.leg_a.liquidity_score() >= self.leg_b.liquidity_score() {
            &self.leg_b
        } else {
            &self.leg_a
        }
    }

    /// Check if opportunity is still valid
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at && self.expected_profit > Decimal::ZERO
    }
}

/// Execution record for persistence and recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub id: Uuid,
    pub opportunity_id: Uuid,
    pub state: ExecutionState,
    pub leg_a: CrossPlatformLeg,
    pub leg_b: CrossPlatformLeg,
    pub started_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    /// Number of hedge retries attempted
    pub hedge_retry_count: u32,
}

impl ExecutionRecord {
    pub fn new(opportunity: &CrossPlatformOpportunity) -> Self {
        Self {
            id: Uuid::new_v4(),
            opportunity_id: opportunity.id,
            state: ExecutionState::Pending,
            leg_a: opportunity.leg_a.clone(),
            leg_b: opportunity.leg_b.clone(),
            started_at: Utc::now(),
            last_updated: Utc::now(),
            hedge_retry_count: 0,
        }
    }

    /// Check if execution needs recovery
    pub fn needs_recovery(&self) -> bool {
        matches!(
            self.state,
            ExecutionState::FirstLegSubmitted { .. } |
            ExecutionState::FirstLegPartialFill { .. } |
            ExecutionState::FirstLegFilled { .. } |
            ExecutionState::HedgeLegSubmitted { .. } |
            ExecutionState::HedgeLegPartialFill { .. } |
            ExecutionState::Unwinding { .. }
        )
    }
}

/// Cross-Platform Execution Engine
pub struct CrossPlatformExecutor {
    config: CrossPlatformConfig,
    polymarket: Arc<ClobClient>,
    kalshi: Arc<KalshiClient>,
    opinion: Arc<OpinionClient>,
    /// Active executions
    executions: Arc<RwLock<HashMap<Uuid, ExecutionRecord>>>,
    /// Execution lock to prevent concurrent executions of same opportunity
    execution_lock: Arc<Mutex<()>>,
    /// Current hedge prices for monitoring
    hedge_price_cache: Arc<RwLock<HashMap<String, (Price, DateTime<Utc>)>>>,
}

impl CrossPlatformExecutor {
    pub fn new(
        config: CrossPlatformConfig,
        polymarket: Arc<ClobClient>,
        kalshi: Arc<KalshiClient>,
        opinion: Arc<OpinionClient>,
    ) -> Self {
        Self {
            config,
            polymarket,
            kalshi,
            opinion,
            executions: Arc::new(RwLock::new(HashMap::new())),
            execution_lock: Arc::new(Mutex::new(())),
            hedge_price_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Execute a cross-platform arbitrage opportunity
    ///
    /// This is the main entry point that handles the full execution lifecycle:
    /// 1. Submit first leg order
    /// 2. Monitor for fill
    /// 3. On fill, submit hedge leg with aggressive pricing
    /// 4. Handle partial fills and slippage
    /// 5. Unwind if hedge fails
    pub async fn execute(&self, opportunity: CrossPlatformOpportunity) -> Result<ExecutionRecord> {
        // Prevent concurrent executions
        let _lock = self.execution_lock.lock().await;

        // Validate opportunity is still valid
        if !opportunity.is_valid() {
            return Err(ArbitrageError::Execution(
                "Opportunity expired before execution".to_string()
            ));
        }

        // Create execution record
        let mut record = ExecutionRecord::new(&opportunity);
        self.save_execution(&record).await;

        info!(
            "Starting cross-platform execution {} for opportunity {}",
            record.id, opportunity.id
        );
        info!(
            "First leg: {} {} {} @ {} (liquidity: {})",
            opportunity.first_leg().venue,
            opportunity.first_leg().side,
            opportunity.first_leg().outcome,
            opportunity.first_leg().target_price,
            opportunity.first_leg().available_liquidity
        );
        info!(
            "Hedge leg: {} {} {} @ {} (liquidity: {})",
            opportunity.hedge_leg().venue,
            opportunity.hedge_leg().side,
            opportunity.hedge_leg().outcome,
            opportunity.hedge_leg().target_price,
            opportunity.hedge_leg().available_liquidity
        );

        // Phase 1: Execute first leg
        let first_leg_result = self.execute_first_leg(&mut record, &opportunity).await;

        match first_leg_result {
            Ok(filled_leg) => {
                info!(
                    "First leg filled: {} @ {} (qty: {})",
                    filled_leg.order_id, filled_leg.avg_price, filled_leg.filled_qty
                );

                // Phase 2: Execute hedge leg
                let hedge_result = self.execute_hedge_leg(&mut record, &opportunity, &filled_leg).await;

                match hedge_result {
                    Ok(hedge_leg) => {
                        // SUCCESS - Both legs filled
                        let realized_profit = self.calculate_realized_profit(&filled_leg, &hedge_leg);

                        record.state = ExecutionState::Completed {
                            first_leg: filled_leg,
                            hedge_leg,
                            realized_profit,
                            completed_at: Utc::now(),
                        };

                        info!(
                            "Cross-platform arbitrage completed! Profit: ${:.4}",
                            realized_profit
                        );
                    }
                    Err(e) => {
                        // Hedge failed - need to unwind
                        warn!("Hedge leg failed: {}. Initiating unwind...", e);

                        let partial_hedge = self.extract_partial_hedge(&record);
                        record.state = ExecutionState::Unwinding {
                            first_leg: filled_leg.clone(),
                            partial_hedge: partial_hedge.clone(),
                            unwind_reason: e.to_string(),
                        };
                        self.save_execution(&record).await;

                        // Execute unwind
                        match self.unwind_position(&mut record, &filled_leg, partial_hedge.as_ref()).await {
                            Ok(unwind_result) => {
                                record.state = unwind_result;
                            }
                            Err(unwind_err) => {
                                error!("Unwind failed: {}. Manual intervention required!", unwind_err);
                                record.state = ExecutionState::Failed {
                                    error: format!("Hedge failed: {}. Unwind failed: {}", e, unwind_err),
                                    first_leg: Some(filled_leg),
                                    hedge_leg: partial_hedge,
                                    failed_at: Utc::now(),
                                };
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("First leg failed: {}", e);
                record.state = ExecutionState::Aborted {
                    reason: AbortReason::FirstLegTimeout,
                    first_leg: None,
                    hedge_leg: None,
                    aborted_at: Utc::now(),
                };
            }
        }

        self.save_execution(&record).await;
        Ok(record)
    }

    /// Execute the first leg of the arbitrage
    async fn execute_first_leg(
        &self,
        record: &mut ExecutionRecord,
        opportunity: &CrossPlatformOpportunity,
    ) -> Result<FilledLegInfo> {
        let leg = opportunity.first_leg();

        // Submit order based on venue
        let order_id = self.submit_order(leg).await?;

        record.state = ExecutionState::FirstLegSubmitted {
            venue: leg.venue,
            order_id: order_id.clone(),
            submitted_at: Utc::now(),
        };
        self.save_execution(record).await;

        // Wait for fill with timeout
        let fill_result = self.wait_for_fill(
            leg.venue,
            &order_id,
            std::time::Duration::from_millis(self.config.first_leg_timeout_ms),
        ).await;

        match fill_result {
            Ok((filled_qty, avg_price, fees)) => {
                // Check if fill is sufficient
                let fill_pct = filled_qty / leg.target_qty.as_decimal();

                if fill_pct < self.config.min_first_leg_fill_pct {
                    // Partial fill too small - cancel and abort
                    let _ = self.cancel_order(leg.venue, &order_id).await;

                    record.state = ExecutionState::Aborted {
                        reason: AbortReason::InsufficientFirstLegFill { fill_pct },
                        first_leg: None,
                        hedge_leg: None,
                        aborted_at: Utc::now(),
                    };

                    return Err(ArbitrageError::Execution(format!(
                        "First leg fill too small: {:.1}% (min: {:.1}%)",
                        fill_pct * dec!(100),
                        self.config.min_first_leg_fill_pct * dec!(100)
                    )));
                }

                Ok(FilledLegInfo {
                    venue: leg.venue,
                    order_id,
                    market_id: leg.market_id.clone(),
                    token_id: leg.token_id.clone(),
                    side: leg.side,
                    outcome: leg.outcome,
                    filled_qty,
                    avg_price,
                    fees,
                    filled_at: Utc::now(),
                })
            }
            Err(e) => {
                // Timeout or error - cancel order
                let _ = self.cancel_order(leg.venue, &order_id).await;
                Err(e)
            }
        }
    }

    /// Execute the hedge leg with aggressive pricing and retries
    async fn execute_hedge_leg(
        &self,
        record: &mut ExecutionRecord,
        opportunity: &CrossPlatformOpportunity,
        first_leg: &FilledLegInfo,
    ) -> Result<FilledLegInfo> {
        let leg = opportunity.hedge_leg();

        // Adjust quantity to match first leg fill
        let hedge_qty = first_leg.filled_qty;

        // Check current hedge price hasn't moved too much
        let current_hedge_price = self.get_current_price(leg.venue, &leg.market_id, &leg.token_id, leg.side).await?;
        let price_move = (current_hedge_price.as_decimal() - leg.target_price.as_decimal()).abs();

        if price_move > self.config.abort_price_threshold {
            return Err(ArbitrageError::Execution(format!(
                "Hedge price moved {:.2}% (threshold: {:.2}%)",
                price_move * dec!(100),
                self.config.abort_price_threshold * dec!(100)
            )));
        }

        // Calculate aggressive hedge price (improve price to increase fill probability)
        let aggressive_price = self.calculate_aggressive_hedge_price(
            current_hedge_price,
            leg.side,
        );

        info!(
            "Hedge leg: current price {}, aggressive price {}",
            current_hedge_price, aggressive_price
        );

        // Retry loop for hedge
        let mut last_error = None;
        let mut partial_fill: Option<FilledLegInfo> = None;

        for attempt in 0..=self.config.max_hedge_retries {
            record.hedge_retry_count = attempt;
            self.save_execution(record).await;

            if attempt > 0 {
                info!("Hedge retry {} of {}", attempt, self.config.max_hedge_retries);
                sleep(std::time::Duration::from_millis(self.config.hedge_retry_delay_ms)).await;

                // Re-check price on retry
                let new_price = self.get_current_price(leg.venue, &leg.market_id, &leg.token_id, leg.side).await?;
                let new_move = (new_price.as_decimal() - leg.target_price.as_decimal()).abs();

                if new_move > self.config.max_hedge_slippage {
                    return Err(ArbitrageError::Execution(format!(
                        "Hedge slippage too high: {:.2}% (max: {:.2}%)",
                        new_move * dec!(100),
                        self.config.max_hedge_slippage * dec!(100)
                    )));
                }
            }

            // Determine remaining quantity to hedge
            let remaining_qty = if let Some(ref pf) = partial_fill {
                hedge_qty - pf.filled_qty
            } else {
                hedge_qty
            };

            if remaining_qty <= Decimal::ZERO {
                break;
            }

            // Submit hedge order
            let order_type = if self.config.use_ioc_for_hedge {
                OrderType::Fok  // Fill-or-kill for hedges
            } else {
                OrderType::Gtc
            };

            let hedge_leg = CrossPlatformLeg {
                venue: leg.venue,
                market_id: leg.market_id.clone(),
                token_id: leg.token_id.clone(),
                side: leg.side,
                outcome: leg.outcome,
                target_price: aggressive_price,
                target_qty: Quantity::new(remaining_qty).unwrap_or(Quantity::ZERO),
                is_hedge: true,
                estimated_latency_ms: leg.estimated_latency_ms,
                available_liquidity: leg.available_liquidity,
            };

            let order_id = match self.submit_order(&hedge_leg).await {
                Ok(id) => id,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            record.state = ExecutionState::HedgeLegSubmitted {
                first_leg: first_leg.clone(),
                hedge_venue: leg.venue,
                hedge_order_id: order_id.clone(),
                submitted_at: Utc::now(),
            };
            self.save_execution(record).await;

            // Wait for fill
            match self.wait_for_fill(
                leg.venue,
                &order_id,
                std::time::Duration::from_millis(self.config.hedge_leg_timeout_ms),
            ).await {
                Ok((filled_qty, avg_price, fees)) => {
                    if filled_qty >= remaining_qty * dec!(0.99) {
                        // Fully filled (allowing for rounding)
                        let total_filled = partial_fill.as_ref()
                            .map(|pf| pf.filled_qty)
                            .unwrap_or(Decimal::ZERO) + filled_qty;

                        let total_fees = partial_fill.as_ref()
                            .map(|pf| pf.fees)
                            .unwrap_or(Decimal::ZERO) + fees;

                        return Ok(FilledLegInfo {
                            venue: leg.venue,
                            order_id,
                            market_id: leg.market_id.clone(),
                            token_id: leg.token_id.clone(),
                            side: leg.side,
                            outcome: leg.outcome,
                            filled_qty: total_filled,
                            avg_price,
                            fees: total_fees,
                            filled_at: Utc::now(),
                        });
                    } else if filled_qty > Decimal::ZERO {
                        // Partial fill - accumulate and retry
                        partial_fill = Some(FilledLegInfo {
                            venue: leg.venue,
                            order_id: order_id.clone(),
                            market_id: leg.market_id.clone(),
                            token_id: leg.token_id.clone(),
                            side: leg.side,
                            outcome: leg.outcome,
                            filled_qty: partial_fill.as_ref()
                                .map(|pf| pf.filled_qty + filled_qty)
                                .unwrap_or(filled_qty),
                            avg_price,
                            fees: partial_fill.as_ref()
                                .map(|pf| pf.fees + fees)
                                .unwrap_or(fees),
                            filled_at: Utc::now(),
                        });

                        record.state = ExecutionState::HedgeLegPartialFill {
                            first_leg: first_leg.clone(),
                            hedge_venue: leg.venue,
                            hedge_order_id: order_id,
                            hedge_filled_qty: partial_fill.as_ref().unwrap().filled_qty,
                            hedge_remaining_qty: hedge_qty - partial_fill.as_ref().unwrap().filled_qty,
                            hedge_avg_price: avg_price,
                        };
                    }
                }
                Err(e) => {
                    let _ = self.cancel_order(leg.venue, &order_id).await;
                    last_error = Some(e);
                }
            }
        }

        // Check if we got partial fill that's acceptable
        if let Some(pf) = partial_fill {
            let fill_pct = pf.filled_qty / hedge_qty;
            if fill_pct >= self.config.min_first_leg_fill_pct {
                warn!(
                    "Hedge only partially filled ({:.1}%), but above threshold",
                    fill_pct * dec!(100)
                );
                return Ok(pf);
            }
        }

        Err(last_error.unwrap_or_else(||
            ArbitrageError::Execution("Hedge leg failed after all retries".to_string())
        ))
    }

    /// Unwind a position when hedge fails
    ///
    /// This attempts to close out the first leg position to minimize loss
    async fn unwind_position(
        &self,
        record: &mut ExecutionRecord,
        first_leg: &FilledLegInfo,
        partial_hedge: Option<&FilledLegInfo>,
    ) -> Result<ExecutionState> {
        info!("Unwinding position for first leg: {} @ {}", first_leg.order_id, first_leg.avg_price);

        // Calculate net position to unwind
        let hedge_filled = partial_hedge
            .map(|h| h.filled_qty)
            .unwrap_or(Decimal::ZERO);
        let unwind_qty = first_leg.filled_qty - hedge_filled;

        if unwind_qty <= Decimal::ZERO {
            // Already fully hedged
            return Ok(ExecutionState::UnwindCompleted {
                first_leg: first_leg.clone(),
                partial_hedge: partial_hedge.cloned(),
                unwind_trades: vec![],
                net_pnl: self.calculate_net_pnl(first_leg, partial_hedge, &[]),
                completed_at: Utc::now(),
            });
        }

        // Unwind by selling what we bought (or buying what we sold)
        let unwind_side = first_leg.side.opposite();

        // Get current market price for unwind
        let current_price = self.get_current_price(
            first_leg.venue,
            &first_leg.market_id,
            &first_leg.token_id,
            unwind_side,
        ).await?;

        // Submit unwind order at market
        let unwind_leg = CrossPlatformLeg {
            venue: first_leg.venue,
            market_id: first_leg.market_id.clone(),
            token_id: first_leg.token_id.clone(),
            side: unwind_side,
            outcome: first_leg.outcome,
            target_price: current_price,
            target_qty: Quantity::new(unwind_qty).unwrap_or(Quantity::ZERO),
            is_hedge: false,
            estimated_latency_ms: 100,
            available_liquidity: Decimal::MAX,
        };

        let unwind_order_id = self.submit_order(&unwind_leg).await?;

        // Wait for unwind fill
        match self.wait_for_fill(
            first_leg.venue,
            &unwind_order_id,
            std::time::Duration::from_millis(10000),  // 10s timeout for unwind
        ).await {
            Ok((filled_qty, avg_price, fees)) => {
                let unwind_fill = FilledLegInfo {
                    venue: first_leg.venue,
                    order_id: unwind_order_id,
                    market_id: first_leg.market_id.clone(),
                    token_id: first_leg.token_id.clone(),
                    side: unwind_side,
                    outcome: first_leg.outcome,
                    filled_qty,
                    avg_price,
                    fees,
                    filled_at: Utc::now(),
                };

                let net_pnl = self.calculate_net_pnl(first_leg, partial_hedge, &[unwind_fill.clone()]);

                info!("Unwind completed. Net P&L: ${:.4}", net_pnl);

                Ok(ExecutionState::UnwindCompleted {
                    first_leg: first_leg.clone(),
                    partial_hedge: partial_hedge.cloned(),
                    unwind_trades: vec![unwind_fill],
                    net_pnl,
                    completed_at: Utc::now(),
                })
            }
            Err(e) => {
                error!("Unwind failed: {}. Position remains open!", e);
                Err(e)
            }
        }
    }

    /// Submit an order to the appropriate venue
    async fn submit_order(&self, leg: &CrossPlatformLeg) -> Result<String> {
        match leg.venue {
            Venue::Polymarket => {
                let order = self.polymarket.place_order(
                    &leg.token_id,
                    leg.side,
                    leg.target_price,
                    leg.target_qty,
                    if leg.is_hedge { OrderType::Fok } else { OrderType::Gtc },
                ).await?;
                Ok(order.id)
            }
            Venue::Kalshi => {
                let order = self.kalshi.place_order(
                    &leg.market_id,
                    leg.side,
                    leg.outcome,
                    leg.target_price,
                    leg.target_qty,
                    if leg.is_hedge { OrderType::Fok } else { OrderType::Gtc },
                ).await?;
                Ok(order.order_id)
            }
            Venue::Opinion => {
                let action = OpinionAction::from_side_outcome(leg.side, leg.outcome);
                let order = self.opinion.place_order(
                    &leg.market_id,
                    &leg.token_id,
                    action,
                    leg.target_price,
                    leg.target_qty,
                ).await?;
                Ok(order.order_id)
            }
        }
    }

    /// Wait for an order to fill
    async fn wait_for_fill(
        &self,
        venue: Venue,
        order_id: &str,
        timeout_duration: std::time::Duration,
    ) -> Result<(Decimal, Decimal, Decimal)> {
        let start = std::time::Instant::now();
        let poll_interval = std::time::Duration::from_millis(50);

        while start.elapsed() < timeout_duration {
            let status = self.get_order_status(venue, order_id).await?;

            match status {
                OrderFillStatus::Filled { qty, avg_price, fees } => {
                    return Ok((qty, avg_price, fees));
                }
                OrderFillStatus::PartialFill { filled_qty, avg_price, fees, .. } => {
                    // For IOC/FOK orders, partial fill is the final state
                    if filled_qty > Decimal::ZERO {
                        return Ok((filled_qty, avg_price, fees));
                    }
                }
                OrderFillStatus::Cancelled | OrderFillStatus::Rejected => {
                    return Err(ArbitrageError::Execution("Order cancelled or rejected".to_string()));
                }
                OrderFillStatus::Pending => {
                    // Keep waiting
                }
            }

            sleep(poll_interval).await;
        }

        Err(ArbitrageError::Execution(format!(
            "Order {} timed out after {:?}",
            order_id, timeout_duration
        )))
    }

    /// Get current order status from venue
    async fn get_order_status(&self, venue: Venue, order_id: &str) -> Result<OrderFillStatus> {
        match venue {
            Venue::Polymarket => {
                let order = self.polymarket.get_order(order_id).await?;
                match order.status {
                    OrderStatus::Matched => Ok(OrderFillStatus::Filled {
                        qty: order.size_matched.unwrap_or(Decimal::ZERO),
                        avg_price: order.price.unwrap_or(Decimal::ZERO),
                        fees: order.fee.unwrap_or(Decimal::ZERO),
                    }),
                    OrderStatus::PartiallyFilled => Ok(OrderFillStatus::PartialFill {
                        filled_qty: order.size_matched.unwrap_or(Decimal::ZERO),
                        remaining_qty: order.original_size.unwrap_or(Decimal::ZERO)
                            - order.size_matched.unwrap_or(Decimal::ZERO),
                        avg_price: order.price.unwrap_or(Decimal::ZERO),
                        fees: order.fee.unwrap_or(Decimal::ZERO),
                    }),
                    OrderStatus::Cancelled => Ok(OrderFillStatus::Cancelled),
                    OrderStatus::Rejected => Ok(OrderFillStatus::Rejected),
                    _ => Ok(OrderFillStatus::Pending),
                }
            }
            Venue::Kalshi => {
                let order = self.kalshi.get_order(order_id).await?;
                if order.is_filled() {
                    let price_cents = order.yes_price.unwrap_or(order.no_price.unwrap_or(0));
                    Ok(OrderFillStatus::Filled {
                        qty: Decimal::from(order.count - order.remaining_count),
                        avg_price: Decimal::from(price_cents) / dec!(100),
                        fees: Decimal::ZERO,  // Kalshi includes fees in price
                    })
                } else if order.is_partial() {
                    let price_cents = order.yes_price.unwrap_or(order.no_price.unwrap_or(0));
                    Ok(OrderFillStatus::PartialFill {
                        filled_qty: Decimal::from(order.count - order.remaining_count),
                        remaining_qty: Decimal::from(order.remaining_count),
                        avg_price: Decimal::from(price_cents) / dec!(100),
                        fees: Decimal::ZERO,
                    })
                } else if order.status == "canceled" || order.status == "cancelled" {
                    Ok(OrderFillStatus::Cancelled)
                } else {
                    Ok(OrderFillStatus::Pending)
                }
            }
            Venue::Opinion => {
                let order = self.opinion.get_order(order_id).await?;
                if order.is_filled() {
                    Ok(OrderFillStatus::Filled {
                        qty: order.matched_size,
                        avg_price: order.price,
                        fees: Decimal::ZERO,  // Commission handled separately
                    })
                } else if order.is_partial() {
                    Ok(OrderFillStatus::PartialFill {
                        filled_qty: order.matched_size,
                        remaining_qty: order.remaining_size,
                        avg_price: order.price,
                        fees: Decimal::ZERO,
                    })
                } else if order.status == "CANCELLED" || order.status == "LAPSED" {
                    Ok(OrderFillStatus::Cancelled)
                } else {
                    Ok(OrderFillStatus::Pending)
                }
            }
        }
    }

    /// Cancel an order on a venue
    async fn cancel_order(&self, venue: Venue, order_id: &str) -> Result<()> {
        match venue {
            Venue::Polymarket => {
                self.polymarket.cancel_order(order_id).await?;
            }
            Venue::Kalshi => {
                self.kalshi.cancel_order(order_id).await?;
            }
            Venue::Opinion => {
                self.opinion.cancel_order(order_id).await?;
            }
        }
        Ok(())
    }

    /// Get current market price for a position
    async fn get_current_price(
        &self,
        venue: Venue,
        market_id: &str,
        token_id: &str,
        side: Side,
    ) -> Result<Price> {
        match venue {
            Venue::Polymarket => {
                let orderbook = self.polymarket.get_orderbook(token_id).await?;
                let price = match side {
                    Side::Buy => orderbook.asks.first().map(|l| l.price),
                    Side::Sell => orderbook.bids.first().map(|l| l.price),
                };
                price.ok_or_else(|| ArbitrageError::Execution("No price available".to_string()))
                    .and_then(|p| Price::new(p).ok_or_else(||
                        ArbitrageError::Execution("Invalid price".to_string())
                    ))
            }
            Venue::Kalshi => {
                let market = self.kalshi.get_market(market_id).await?;
                let price = match side {
                    Side::Buy => market.yes_ask_price(),
                    Side::Sell => market.yes_bid_price(),
                };
                price.ok_or_else(|| ArbitrageError::Execution("No price available".to_string()))
            }
            Venue::Opinion => {
                let orderbook = self.opinion.get_orderbook(market_id, token_id).await?;
                let price = match side {
                    Side::Buy => orderbook.best_back().map(|l| l.price),
                    Side::Sell => orderbook.best_lay().map(|l| l.price),
                };
                price.and_then(|p| Price::new(p))
                    .ok_or_else(|| ArbitrageError::Execution("No price available".to_string()))
            }
        }
    }

    /// Calculate aggressive price for hedge leg
    fn calculate_aggressive_hedge_price(&self, current_price: Price, side: Side) -> Price {
        let adjustment = self.config.hedge_price_aggression;
        let new_price = match side {
            Side::Buy => current_price.as_decimal() + adjustment,  // Pay more
            Side::Sell => current_price.as_decimal() - adjustment, // Accept less
        };
        Price::new(new_price.max(dec!(0.01)).min(dec!(0.99)))
            .unwrap_or(current_price)
    }

    /// Calculate realized profit from completed arbitrage
    fn calculate_realized_profit(&self, first_leg: &FilledLegInfo, hedge_leg: &FilledLegInfo) -> Decimal {
        let first_cost = match first_leg.side {
            Side::Buy => first_leg.filled_qty * first_leg.avg_price + first_leg.fees,
            Side::Sell => -(first_leg.filled_qty * first_leg.avg_price - first_leg.fees),
        };

        let hedge_cost = match hedge_leg.side {
            Side::Buy => hedge_leg.filled_qty * hedge_leg.avg_price + hedge_leg.fees,
            Side::Sell => -(hedge_leg.filled_qty * hedge_leg.avg_price - hedge_leg.fees),
        };

        // For arb: we want to buy low on one venue and sell high on another
        // Profit = revenue - cost
        -(first_cost + hedge_cost)
    }

    /// Calculate net P&L including unwind
    fn calculate_net_pnl(
        &self,
        first_leg: &FilledLegInfo,
        partial_hedge: Option<&FilledLegInfo>,
        unwind_trades: &[FilledLegInfo],
    ) -> Decimal {
        let mut net = Decimal::ZERO;

        // First leg cost
        net -= match first_leg.side {
            Side::Buy => first_leg.filled_qty * first_leg.avg_price + first_leg.fees,
            Side::Sell => -(first_leg.filled_qty * first_leg.avg_price - first_leg.fees),
        };

        // Partial hedge (if any)
        if let Some(hedge) = partial_hedge {
            net -= match hedge.side {
                Side::Buy => hedge.filled_qty * hedge.avg_price + hedge.fees,
                Side::Sell => -(hedge.filled_qty * hedge.avg_price - hedge.fees),
            };
        }

        // Unwind trades
        for trade in unwind_trades {
            net -= match trade.side {
                Side::Buy => trade.filled_qty * trade.avg_price + trade.fees,
                Side::Sell => -(trade.filled_qty * trade.avg_price - trade.fees),
            };
        }

        net
    }

    /// Extract partial hedge info from current state
    fn extract_partial_hedge(&self, record: &ExecutionRecord) -> Option<FilledLegInfo> {
        match &record.state {
            ExecutionState::HedgeLegPartialFill {
                hedge_venue,
                hedge_order_id,
                hedge_filled_qty,
                hedge_avg_price,
                ..
            } => Some(FilledLegInfo {
                venue: *hedge_venue,
                order_id: hedge_order_id.clone(),
                market_id: record.leg_b.market_id.clone(),
                token_id: record.leg_b.token_id.clone(),
                side: record.leg_b.side,
                outcome: record.leg_b.outcome,
                filled_qty: *hedge_filled_qty,
                avg_price: *hedge_avg_price,
                fees: Decimal::ZERO,
                filled_at: Utc::now(),
            }),
            _ => None,
        }
    }

    /// Save execution record for persistence/recovery
    async fn save_execution(&self, record: &ExecutionRecord) {
        let mut executions = self.executions.write().await;
        executions.insert(record.id, record.clone());
        // TODO: Also persist to database for crash recovery
    }

    /// Load executions that need recovery (called on startup)
    pub async fn load_pending_executions(&self) -> Vec<ExecutionRecord> {
        let executions = self.executions.read().await;
        executions.values()
            .filter(|r| r.needs_recovery())
            .cloned()
            .collect()
    }

    /// Recover a pending execution
    pub async fn recover_execution(&self, record: &ExecutionRecord) -> Result<ExecutionRecord> {
        info!("Recovering execution {}", record.id);

        match &record.state {
            ExecutionState::FirstLegSubmitted { venue, order_id, .. } => {
                // Check if order filled while we were down
                let status = self.get_order_status(*venue, order_id).await?;
                // Handle based on status...
                todo!("Implement recovery logic")
            }
            ExecutionState::FirstLegFilled { .. } |
            ExecutionState::HedgeLegSubmitted { .. } |
            ExecutionState::HedgeLegPartialFill { .. } => {
                // Need to complete or unwind hedge
                todo!("Implement hedge recovery logic")
            }
            ExecutionState::Unwinding { first_leg, partial_hedge, .. } => {
                // Resume unwind
                let mut record = record.clone();
                self.unwind_position(&mut record, first_leg, partial_hedge.as_ref()).await?;
                Ok(record)
            }
            _ => Ok(record.clone()),
        }
    }
}

/// Order fill status
#[derive(Debug, Clone)]
enum OrderFillStatus {
    Pending,
    PartialFill {
        filled_qty: Decimal,
        remaining_qty: Decimal,
        avg_price: Decimal,
        fees: Decimal,
    },
    Filled {
        qty: Decimal,
        avg_price: Decimal,
        fees: Decimal,
    },
    Cancelled,
    Rejected,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_leg_liquidity_score() {
        let leg_a = CrossPlatformLeg {
            venue: Venue::Polymarket,
            market_id: "market1".to_string(),
            token_id: "token1".to_string(),
            side: Side::Buy,
            outcome: Outcome::Yes,
            target_price: Price::new(dec!(0.50)).unwrap(),
            target_qty: Quantity::new(dec!(100)).unwrap(),
            is_hedge: false,
            estimated_latency_ms: 50,
            available_liquidity: dec!(1000),
        };

        let leg_b = CrossPlatformLeg {
            venue: Venue::Kalshi,
            market_id: "market2".to_string(),
            token_id: "token2".to_string(),
            side: Side::Sell,
            outcome: Outcome::Yes,
            target_price: Price::new(dec!(0.52)).unwrap(),
            target_qty: Quantity::new(dec!(100)).unwrap(),
            is_hedge: false,
            estimated_latency_ms: 100,
            available_liquidity: dec!(500),
        };

        // leg_a should have higher score (more liquidity, lower latency)
        assert!(leg_a.liquidity_score() > leg_b.liquidity_score());
    }

    #[test]
    fn test_opportunity_first_leg_selection() {
        let leg_a = CrossPlatformLeg {
            venue: Venue::Polymarket,
            market_id: "market1".to_string(),
            token_id: "token1".to_string(),
            side: Side::Buy,
            outcome: Outcome::Yes,
            target_price: Price::new(dec!(0.50)).unwrap(),
            target_qty: Quantity::new(dec!(100)).unwrap(),
            is_hedge: false,
            estimated_latency_ms: 50,
            available_liquidity: dec!(1000),
        };

        let leg_b = CrossPlatformLeg {
            venue: Venue::Kalshi,
            market_id: "market2".to_string(),
            token_id: "token2".to_string(),
            side: Side::Sell,
            outcome: Outcome::Yes,
            target_price: Price::new(dec!(0.52)).unwrap(),
            target_qty: Quantity::new(dec!(100)).unwrap(),
            is_hedge: false,
            estimated_latency_ms: 100,
            available_liquidity: dec!(500),
        };

        let opportunity = CrossPlatformOpportunity {
            id: Uuid::new_v4(),
            leg_a: leg_a.clone(),
            leg_b: leg_b.clone(),
            expected_profit: dec!(2.0),
            expected_profit_pct: dec!(0.04),
            required_capital: dec!(50),
            detected_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(10),
            correlation_score: 0.95,
        };

        // leg_a (Polymarket) should be first due to higher liquidity score
        assert_eq!(opportunity.first_leg().venue, Venue::Polymarket);
        assert_eq!(opportunity.hedge_leg().venue, Venue::Kalshi);
    }
}
