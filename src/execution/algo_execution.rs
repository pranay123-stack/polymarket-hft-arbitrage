//! Algorithmic Execution Module - TWAP/VWAP/POV
//!
//! Provides sophisticated execution algorithms for minimizing market impact
//! when executing large orders across prediction markets.
//!
//! ## Algorithms Implemented:
//!
//! ### TWAP (Time-Weighted Average Price)
//! - Splits order into equal slices over time
//! - Executes at regular intervals
//! - Simple but effective for reducing market impact
//! - Best when volume profile is flat or unknown
//!
//! ### VWAP (Volume-Weighted Average Price)
//! - Splits order based on historical volume profile
//! - Executes more during high-volume periods
//! - Better participation rate distribution
//! - Requires historical volume data
//!
//! ### POV (Percentage of Volume)
//! - Participates as a percentage of market volume
//! - Adapts to actual market conditions
//! - Prevents excessive market share
//! - Most adaptive but unpredictable completion time
//!
//! ## Usage Considerations:
//! - TWAP: Best for predictable execution with known end time
//! - VWAP: Best for minimizing market impact relative to volume
//! - POV: Best when completion time is flexible

use crate::api::{
    ClobClient,
    kalshi::KalshiClient,
    opinion::{OpinionClient, OpinionAction},
};
use crate::core::{
    error::{ArbitrageError, Result},
    types::{Order, OrderStatus, OrderType, Outcome, Price, Quantity, Side},
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::Venue;

/// Algorithm type for execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlgoType {
    /// Time-Weighted Average Price
    TWAP,
    /// Volume-Weighted Average Price
    VWAP,
    /// Percentage of Volume
    POV,
    /// Aggressive - fill as fast as possible within constraints
    Aggressive,
    /// Passive - use limit orders, be patient
    Passive,
}

/// Configuration for algorithmic execution
#[derive(Debug, Clone)]
pub struct AlgoConfig {
    /// Algorithm type
    pub algo_type: AlgoType,
    /// Venue to execute on
    pub venue: Venue,
    /// Market identifier
    pub market_id: String,
    /// Token identifier
    pub token_id: String,
    /// Order side
    pub side: Side,
    /// Outcome
    pub outcome: Outcome,
    /// Total quantity to execute
    pub total_quantity: Quantity,
    /// Limit price (worst acceptable price)
    pub limit_price: Price,
    /// Start time for execution
    pub start_time: DateTime<Utc>,
    /// End time for execution (must complete by this time)
    pub end_time: DateTime<Utc>,
    /// Number of slices to split the order into
    pub num_slices: u32,
    /// Minimum order size per slice
    pub min_slice_size: Quantity,
    /// Maximum price deviation allowed per slice (from limit)
    pub max_slice_deviation: Decimal,
    /// For POV: target participation rate (0.0 - 1.0)
    pub target_participation_rate: Decimal,
    /// Whether to be aggressive on last slice
    pub aggressive_finish: bool,
    /// Randomize slice sizes slightly to avoid detection
    pub randomize: bool,
}

impl Default for AlgoConfig {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            algo_type: AlgoType::TWAP,
            venue: Venue::Polymarket,
            market_id: String::new(),
            token_id: String::new(),
            side: Side::Buy,
            outcome: Outcome::Yes,
            total_quantity: Quantity::ZERO,
            limit_price: Price::new_unchecked(dec!(0.50)),
            start_time: now,
            end_time: now + Duration::minutes(30),
            num_slices: 10,
            min_slice_size: Quantity::new(dec!(10)).unwrap_or(Quantity::ZERO),
            max_slice_deviation: dec!(0.02),  // 2% from limit
            target_participation_rate: dec!(0.15),  // 15% of volume
            aggressive_finish: true,
            randomize: true,
        }
    }
}

/// State of an algorithmic execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlgoState {
    /// Not started yet
    Pending,
    /// Currently executing
    Running {
        slices_completed: u32,
        slices_total: u32,
        filled_quantity: Decimal,
        avg_fill_price: Decimal,
    },
    /// Temporarily paused
    Paused {
        reason: String,
        resume_at: Option<DateTime<Utc>>,
    },
    /// Successfully completed
    Completed {
        total_filled: Decimal,
        avg_fill_price: Decimal,
        slippage_bps: Decimal,
        completed_at: DateTime<Utc>,
    },
    /// Failed
    Failed {
        error: String,
        partial_filled: Decimal,
        failed_at: DateTime<Utc>,
    },
    /// Cancelled by user
    Cancelled {
        partial_filled: Decimal,
        cancelled_at: DateTime<Utc>,
    },
}

/// A single slice/child order in the algo execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgoSlice {
    pub slice_id: u32,
    pub target_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub target_time: DateTime<Utc>,
    pub executed_at: Option<DateTime<Utc>>,
    pub fill_price: Option<Decimal>,
    pub order_id: Option<String>,
    pub status: SliceStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceStatus {
    Pending,
    Submitted,
    PartialFill,
    Filled,
    Failed,
    Skipped,
}

/// Result of an algorithmic execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgoResult {
    pub id: Uuid,
    pub config: AlgoConfig,
    pub state: AlgoState,
    pub slices: Vec<AlgoSlice>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub execution_quality: ExecutionQuality,
}

/// Execution quality metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionQuality {
    /// Total quantity filled
    pub total_filled: Decimal,
    /// Volume-weighted average price achieved
    pub vwap_achieved: Decimal,
    /// Market VWAP during execution window
    pub market_vwap: Decimal,
    /// Slippage vs limit price (bps)
    pub slippage_vs_limit_bps: Decimal,
    /// Slippage vs market VWAP (bps)
    pub slippage_vs_vwap_bps: Decimal,
    /// Fill rate (0-1)
    pub fill_rate: Decimal,
    /// Participation rate (for POV)
    pub participation_rate: Decimal,
    /// Number of slices executed
    pub slices_executed: u32,
    /// Number of slices failed
    pub slices_failed: u32,
    /// Total execution time (ms)
    pub execution_time_ms: u64,
}

/// Historical volume profile for VWAP calculation
#[derive(Debug, Clone)]
pub struct VolumeProfile {
    /// Volume buckets by time of day (24 buckets for hourly)
    pub hourly_volumes: [Decimal; 24],
    /// Total average daily volume
    pub avg_daily_volume: Decimal,
    /// Last updated
    pub updated_at: DateTime<Utc>,
}

impl Default for VolumeProfile {
    fn default() -> Self {
        // Default flat profile
        let hourly = [Decimal::ONE / dec!(24); 24];
        Self {
            hourly_volumes: hourly,
            avg_daily_volume: dec!(10000),
            updated_at: Utc::now(),
        }
    }
}

impl VolumeProfile {
    /// Get the expected volume share for a given hour
    pub fn volume_share_for_hour(&self, hour: u32) -> Decimal {
        if hour >= 24 {
            return dec!(0);
        }
        self.hourly_volumes[hour as usize]
    }

    /// Calculate expected volume for a time window
    pub fn expected_volume(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Decimal {
        let start_hour = start.hour();
        let end_hour = end.hour();
        let duration_hours = (end - start).num_hours().max(1) as u32;

        let mut total_share = dec!(0);
        for h in 0..duration_hours {
            let hour = (start_hour + h) % 24;
            total_share += self.volume_share_for_hour(hour);
        }

        self.avg_daily_volume * total_share
    }
}

/// Algorithmic Execution Engine
pub struct AlgoExecutionEngine {
    polymarket: Arc<ClobClient>,
    kalshi: Arc<KalshiClient>,
    opinion: Arc<OpinionClient>,
    /// Active executions
    executions: Arc<RwLock<std::collections::HashMap<Uuid, AlgoExecution>>>,
    /// Volume profiles by market
    volume_profiles: Arc<RwLock<std::collections::HashMap<String, VolumeProfile>>>,
    /// Shutdown signal
    shutdown: Arc<RwLock<bool>>,
}

/// An active algorithmic execution
struct AlgoExecution {
    id: Uuid,
    config: AlgoConfig,
    state: Arc<RwLock<AlgoState>>,
    slices: Arc<RwLock<Vec<AlgoSlice>>>,
    cancel_tx: mpsc::Sender<()>,
}

impl AlgoExecutionEngine {
    pub fn new(
        polymarket: Arc<ClobClient>,
        kalshi: Arc<KalshiClient>,
        opinion: Arc<OpinionClient>,
    ) -> Self {
        Self {
            polymarket,
            kalshi,
            opinion,
            executions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            volume_profiles: Arc::new(RwLock::new(std::collections::HashMap::new())),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Start a new algorithmic execution
    pub async fn start_execution(&self, config: AlgoConfig) -> Result<Uuid> {
        let id = Uuid::new_v4();

        // Validate configuration
        self.validate_config(&config)?;

        // Generate slices based on algorithm type
        let slices = match config.algo_type {
            AlgoType::TWAP => self.generate_twap_slices(&config).await,
            AlgoType::VWAP => self.generate_vwap_slices(&config).await?,
            AlgoType::POV => self.generate_pov_slices(&config).await,
            AlgoType::Aggressive => self.generate_aggressive_slices(&config).await,
            AlgoType::Passive => self.generate_passive_slices(&config).await,
        };

        info!(
            "Starting {} execution {} with {} slices for {} {} @ limit {}",
            format!("{:?}", config.algo_type),
            id,
            slices.len(),
            config.total_quantity,
            config.side,
            config.limit_price
        );

        // Create cancel channel
        let (cancel_tx, cancel_rx) = mpsc::channel(1);

        // Create execution state
        let execution = AlgoExecution {
            id,
            config: config.clone(),
            state: Arc::new(RwLock::new(AlgoState::Running {
                slices_completed: 0,
                slices_total: slices.len() as u32,
                filled_quantity: Decimal::ZERO,
                avg_fill_price: Decimal::ZERO,
            })),
            slices: Arc::new(RwLock::new(slices)),
            cancel_tx,
        };

        // Store execution
        {
            let mut execs = self.executions.write().await;
            execs.insert(id, execution);
        }

        // Spawn execution task
        let engine = self.clone_for_task();
        let exec_id = id;
        tokio::spawn(async move {
            engine.run_execution(exec_id, cancel_rx).await;
        });

        Ok(id)
    }

    /// Validate algo configuration
    fn validate_config(&self, config: &AlgoConfig) -> Result<()> {
        if config.total_quantity.as_decimal() <= Decimal::ZERO {
            return Err(ArbitrageError::Execution("Total quantity must be positive".to_string()));
        }

        if config.end_time <= config.start_time {
            return Err(ArbitrageError::Execution("End time must be after start time".to_string()));
        }

        if config.num_slices == 0 {
            return Err(ArbitrageError::Execution("Must have at least 1 slice".to_string()));
        }

        let duration = config.end_time - config.start_time;
        if duration < Duration::seconds(60) {
            return Err(ArbitrageError::Execution("Execution window must be at least 60 seconds".to_string()));
        }

        Ok(())
    }

    /// Generate TWAP slices - equal distribution over time
    async fn generate_twap_slices(&self, config: &AlgoConfig) -> Vec<AlgoSlice> {
        let total_qty = config.total_quantity.as_decimal();
        let num_slices = config.num_slices;
        let duration = config.end_time - config.start_time;
        let interval = duration / num_slices as i32;

        let base_qty = total_qty / Decimal::from(num_slices);
        let mut slices = Vec::with_capacity(num_slices as usize);
        let mut remaining = total_qty;

        for i in 0..num_slices {
            let target_time = config.start_time + interval * i as i32;

            // Apply randomization if enabled
            let qty = if config.randomize && i < num_slices - 1 {
                // Random variation of +/- 20%
                let variation = rand::random::<f64>() * 0.4 - 0.2;
                let varied_qty = base_qty * (Decimal::ONE + Decimal::from_f64_retain(variation).unwrap_or(Decimal::ZERO));
                varied_qty.min(remaining).max(config.min_slice_size.as_decimal())
            } else if i == num_slices - 1 {
                // Last slice gets remaining
                remaining
            } else {
                base_qty.min(remaining)
            };

            remaining -= qty;

            slices.push(AlgoSlice {
                slice_id: i,
                target_quantity: qty,
                filled_quantity: Decimal::ZERO,
                target_time,
                executed_at: None,
                fill_price: None,
                order_id: None,
                status: SliceStatus::Pending,
            });
        }

        slices
    }

    /// Generate VWAP slices - distribution based on historical volume
    async fn generate_vwap_slices(&self, config: &AlgoConfig) -> Result<Vec<AlgoSlice>> {
        let profile = {
            let profiles = self.volume_profiles.read().await;
            profiles.get(&config.market_id).cloned().unwrap_or_default()
        };

        let total_qty = config.total_quantity.as_decimal();
        let duration = config.end_time - config.start_time;
        let interval = duration / config.num_slices as i32;

        let mut slices = Vec::with_capacity(config.num_slices as usize);
        let mut remaining = total_qty;

        // Calculate total expected volume during execution window
        let total_expected_vol = profile.expected_volume(config.start_time, config.end_time);

        for i in 0..config.num_slices {
            let slice_start = config.start_time + interval * i as i32;
            let slice_end = slice_start + interval;

            // Expected volume for this slice
            let slice_expected_vol = profile.expected_volume(slice_start, slice_end);
            let volume_share = if total_expected_vol > Decimal::ZERO {
                slice_expected_vol / total_expected_vol
            } else {
                Decimal::ONE / Decimal::from(config.num_slices)
            };

            // Quantity for this slice based on volume share
            let qty = (total_qty * volume_share)
                .min(remaining)
                .max(config.min_slice_size.as_decimal());

            remaining = (remaining - qty).max(Decimal::ZERO);

            slices.push(AlgoSlice {
                slice_id: i,
                target_quantity: qty,
                filled_quantity: Decimal::ZERO,
                target_time: slice_start,
                executed_at: None,
                fill_price: None,
                order_id: None,
                status: SliceStatus::Pending,
            });
        }

        // Assign any remaining to last slice
        if remaining > Decimal::ZERO && !slices.is_empty() {
            slices.last_mut().unwrap().target_quantity += remaining;
        }

        Ok(slices)
    }

    /// Generate POV slices - adapts to actual volume
    async fn generate_pov_slices(&self, config: &AlgoConfig) -> Vec<AlgoSlice> {
        // POV is reactive, so we create minimal initial slices
        // and generate more dynamically during execution
        let duration = config.end_time - config.start_time;
        let check_interval = Duration::seconds(30);  // Check every 30 seconds
        let num_checks = (duration.num_seconds() / check_interval.num_seconds()) as u32;

        let mut slices = Vec::with_capacity(num_checks as usize);

        for i in 0..num_checks.max(1) {
            let target_time = config.start_time + check_interval * i as i32;

            slices.push(AlgoSlice {
                slice_id: i,
                target_quantity: Decimal::ZERO,  // Will be determined at execution time
                filled_quantity: Decimal::ZERO,
                target_time,
                executed_at: None,
                fill_price: None,
                order_id: None,
                status: SliceStatus::Pending,
            });
        }

        slices
    }

    /// Generate aggressive slices - front-loaded execution
    async fn generate_aggressive_slices(&self, config: &AlgoConfig) -> Vec<AlgoSlice> {
        let total_qty = config.total_quantity.as_decimal();
        let num_slices = config.num_slices.min(5);  // Fewer, larger slices
        let duration = config.end_time - config.start_time;
        let interval = duration / num_slices as i32;

        let mut slices = Vec::with_capacity(num_slices as usize);
        let mut remaining = total_qty;

        // Front-loaded: 50% in first slice, 25% in second, etc.
        let weights = [dec!(0.50), dec!(0.25), dec!(0.15), dec!(0.07), dec!(0.03)];

        for i in 0..num_slices as usize {
            let target_time = config.start_time + interval * i as i32;
            let weight = weights.get(i).copied().unwrap_or(dec!(0.03));
            let qty = (total_qty * weight).min(remaining);
            remaining -= qty;

            slices.push(AlgoSlice {
                slice_id: i as u32,
                target_quantity: qty,
                filled_quantity: Decimal::ZERO,
                target_time,
                executed_at: None,
                fill_price: None,
                order_id: None,
                status: SliceStatus::Pending,
            });
        }

        // Add remaining to last slice
        if remaining > Decimal::ZERO && !slices.is_empty() {
            slices.last_mut().unwrap().target_quantity += remaining;
        }

        slices
    }

    /// Generate passive slices - evenly distributed, patient execution
    async fn generate_passive_slices(&self, config: &AlgoConfig) -> Vec<AlgoSlice> {
        // Same as TWAP but with more slices for better averaging
        let config = AlgoConfig {
            num_slices: config.num_slices * 2,
            ..config.clone()
        };
        self.generate_twap_slices(&config).await
    }

    /// Run an execution (called in spawned task)
    async fn run_execution(&self, id: Uuid, mut cancel_rx: mpsc::Receiver<()>) {
        let execution = {
            let execs = self.executions.read().await;
            match execs.get(&id) {
                Some(e) => AlgoExecutionSnapshot {
                    config: e.config.clone(),
                    state: e.state.clone(),
                    slices: e.slices.clone(),
                },
                None => return,
            }
        };

        let config = execution.config;
        let state = execution.state;
        let slices = execution.slices;

        let mut total_filled = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        let mut slices_completed = 0u32;
        let mut slices_failed = 0u32;

        let start = std::time::Instant::now();

        // Wait until start time
        if Utc::now() < config.start_time {
            let delay = (config.start_time - Utc::now()).to_std().unwrap_or_default();
            tokio::select! {
                _ = sleep(delay) => {}
                _ = cancel_rx.recv() => {
                    self.mark_cancelled(id, total_filled).await;
                    return;
                }
            }
        }

        // Execute slices
        let slice_count = {
            let s = slices.read().await;
            s.len()
        };

        for i in 0..slice_count {
            // Check for cancellation
            if let Ok(_) = cancel_rx.try_recv() {
                self.mark_cancelled(id, total_filled).await;
                return;
            }

            // Check if we've passed end time
            if Utc::now() > config.end_time {
                warn!("Algo {} exceeded end time, stopping", id);
                break;
            }

            // Get slice
            let slice = {
                let s = slices.read().await;
                s.get(i).cloned()
            };

            let mut slice = match slice {
                Some(s) => s,
                None => continue,
            };

            // Wait until slice target time
            if Utc::now() < slice.target_time {
                let delay = (slice.target_time - Utc::now()).to_std().unwrap_or_default();
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = cancel_rx.recv() => {
                        self.mark_cancelled(id, total_filled).await;
                        return;
                    }
                }
            }

            // Skip if quantity is zero (POV handles this differently)
            let slice_qty = if config.algo_type == AlgoType::POV {
                // For POV, calculate based on recent volume
                self.calculate_pov_slice_qty(&config, total_filled).await
            } else {
                slice.target_quantity
            };

            if slice_qty <= Decimal::ZERO {
                slice.status = SliceStatus::Skipped;
                let mut s = slices.write().await;
                if let Some(existing) = s.get_mut(i) {
                    *existing = slice;
                }
                continue;
            }

            // Execute the slice
            slice.status = SliceStatus::Submitted;
            slice.target_quantity = slice_qty;

            debug!(
                "Executing slice {}/{} for {} @ {}",
                i + 1, slice_count, slice_qty, config.limit_price
            );

            match self.execute_slice(&config, slice_qty).await {
                Ok((filled, price)) => {
                    slice.filled_quantity = filled;
                    slice.fill_price = Some(price);
                    slice.executed_at = Some(Utc::now());
                    slice.status = if filled >= slice_qty * dec!(0.95) {
                        SliceStatus::Filled
                    } else {
                        SliceStatus::PartialFill
                    };

                    total_filled += filled;
                    total_cost += filled * price;
                    slices_completed += 1;
                }
                Err(e) => {
                    warn!("Slice {} failed: {}", i, e);
                    slice.status = SliceStatus::Failed;
                    slices_failed += 1;
                }
            }

            // Update slice state
            {
                let mut s = slices.write().await;
                if let Some(existing) = s.get_mut(i) {
                    *existing = slice;
                }
            }

            // Update overall state
            let avg_price = if total_filled > Decimal::ZERO {
                total_cost / total_filled
            } else {
                Decimal::ZERO
            };

            *state.write().await = AlgoState::Running {
                slices_completed,
                slices_total: slice_count as u32,
                filled_quantity: total_filled,
                avg_fill_price: avg_price,
            };

            // Check if we're done
            if total_filled >= config.total_quantity.as_decimal() * dec!(0.99) {
                break;
            }
        }

        // Final state
        let execution_time_ms = start.elapsed().as_millis() as u64;
        let avg_price = if total_filled > Decimal::ZERO {
            total_cost / total_filled
        } else {
            Decimal::ZERO
        };

        let slippage_bps = if config.limit_price.as_decimal() > Decimal::ZERO {
            (avg_price - config.limit_price.as_decimal()) / config.limit_price.as_decimal() * dec!(10000)
        } else {
            Decimal::ZERO
        };

        if total_filled >= config.total_quantity.as_decimal() * dec!(0.95) {
            *state.write().await = AlgoState::Completed {
                total_filled,
                avg_fill_price: avg_price,
                slippage_bps,
                completed_at: Utc::now(),
            };
            info!(
                "Algo {} completed: filled {} @ avg {} (slippage: {}bps) in {}ms",
                id, total_filled, avg_price, slippage_bps, execution_time_ms
            );
        } else {
            *state.write().await = AlgoState::Failed {
                error: format!("Only filled {} of {} ({:.1}%)",
                    total_filled, config.total_quantity, total_filled / config.total_quantity.as_decimal() * dec!(100)),
                partial_filled: total_filled,
                failed_at: Utc::now(),
            };
            warn!(
                "Algo {} failed: only filled {} of {}",
                id, total_filled, config.total_quantity
            );
        }
    }

    /// Execute a single slice
    async fn execute_slice(
        &self,
        config: &AlgoConfig,
        quantity: Decimal,
    ) -> Result<(Decimal, Decimal)> {
        let qty = Quantity::new(quantity).unwrap_or(Quantity::ZERO);

        match config.venue {
            Venue::Polymarket => {
                let order = Order {
                    id: Uuid::new_v4().to_string(),
                    market_id: config.market_id.clone(),
                    token_id: config.token_id.clone(),
                    side: config.side,
                    outcome: config.outcome,
                    price: config.limit_price,
                    quantity: qty,
                    order_type: OrderType::Fok,  // FOK for slices
                    expiration: None,
                    created_at: Utc::now(),
                    client_order_id: None,
                };

                let response = self.polymarket.place_order(&order).await?;

                // Wait for fill
                sleep(std::time::Duration::from_millis(500)).await;

                let status = self.polymarket.get_order(&response.id).await?;
                let filled = status.original_size.parse::<Decimal>().unwrap_or(Decimal::ZERO)
                    - status.remaining_size.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                let price = status.price.parse::<Decimal>().unwrap_or(config.limit_price.as_decimal());

                Ok((filled, price))
            }
            Venue::Kalshi => {
                let response = self.kalshi.place_order(
                    &config.market_id,
                    config.side,
                    config.outcome,
                    config.limit_price,
                    qty,
                    OrderType::Fok,
                ).await?;

                sleep(std::time::Duration::from_millis(500)).await;

                let status = self.kalshi.get_order(&response.order_id).await?;
                let filled = Decimal::from(status.count - status.remaining_count);
                let price_cents = status.yes_price.unwrap_or(status.no_price.unwrap_or(0));
                let price = Decimal::from(price_cents) / dec!(100);

                Ok((filled, price))
            }
            Venue::Opinion => {
                let action = OpinionAction::from_side_outcome(config.side, config.outcome);
                let response = self.opinion.place_order(
                    &config.market_id,
                    &config.token_id,
                    action,
                    config.limit_price,
                    qty,
                ).await?;

                sleep(std::time::Duration::from_millis(500)).await;

                let status = self.opinion.get_order(&response.order_id).await?;
                let filled = status.matched_size;
                let price = status.price;

                Ok((filled, price))
            }
        }
    }

    /// Calculate POV slice quantity based on recent volume
    async fn calculate_pov_slice_qty(
        &self,
        config: &AlgoConfig,
        already_filled: Decimal,
    ) -> Decimal {
        // Get recent market volume (would need to implement volume tracking)
        // For now, use a conservative estimate
        let remaining = config.total_quantity.as_decimal() - already_filled;
        let target_slice = remaining * config.target_participation_rate;

        target_slice
            .min(remaining)
            .max(config.min_slice_size.as_decimal())
    }

    /// Mark execution as cancelled
    async fn mark_cancelled(&self, id: Uuid, partial_filled: Decimal) {
        if let Some(exec) = self.executions.read().await.get(&id) {
            *exec.state.write().await = AlgoState::Cancelled {
                partial_filled,
                cancelled_at: Utc::now(),
            };
        }
        info!("Algo {} cancelled with {} filled", id, partial_filled);
    }

    /// Cancel an active execution
    pub async fn cancel_execution(&self, id: Uuid) -> Result<()> {
        let execs = self.executions.read().await;
        if let Some(exec) = execs.get(&id) {
            let _ = exec.cancel_tx.send(()).await;
            Ok(())
        } else {
            Err(ArbitrageError::Execution(format!("Execution {} not found", id)))
        }
    }

    /// Get execution status
    pub async fn get_execution(&self, id: Uuid) -> Option<AlgoResult> {
        let execs = self.executions.read().await;
        let exec = execs.get(&id)?;

        let state = exec.state.read().await.clone();
        let slices = exec.slices.read().await.clone();

        Some(AlgoResult {
            id,
            config: exec.config.clone(),
            state,
            slices,
            start_time: exec.config.start_time,
            end_time: Some(exec.config.end_time),
            execution_quality: ExecutionQuality::default(),
        })
    }

    /// Update volume profile for a market
    pub async fn update_volume_profile(&self, market_id: &str, profile: VolumeProfile) {
        let mut profiles = self.volume_profiles.write().await;
        profiles.insert(market_id.to_string(), profile);
    }

    /// Clone self for spawned tasks (Arc references)
    fn clone_for_task(&self) -> Self {
        Self {
            polymarket: self.polymarket.clone(),
            kalshi: self.kalshi.clone(),
            opinion: self.opinion.clone(),
            executions: self.executions.clone(),
            volume_profiles: self.volume_profiles.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

/// Snapshot for task execution
struct AlgoExecutionSnapshot {
    config: AlgoConfig,
    state: Arc<RwLock<AlgoState>>,
    slices: Arc<RwLock<Vec<AlgoSlice>>>,
}

impl AlgoConfig {
    /// Create a TWAP configuration
    pub fn twap(
        venue: Venue,
        market_id: String,
        token_id: String,
        side: Side,
        outcome: Outcome,
        quantity: Quantity,
        limit_price: Price,
        duration: Duration,
        num_slices: u32,
    ) -> Self {
        let now = Utc::now();
        Self {
            algo_type: AlgoType::TWAP,
            venue,
            market_id,
            token_id,
            side,
            outcome,
            total_quantity: quantity,
            limit_price,
            start_time: now,
            end_time: now + duration,
            num_slices,
            ..Default::default()
        }
    }

    /// Create a VWAP configuration
    pub fn vwap(
        venue: Venue,
        market_id: String,
        token_id: String,
        side: Side,
        outcome: Outcome,
        quantity: Quantity,
        limit_price: Price,
        duration: Duration,
        num_slices: u32,
    ) -> Self {
        let mut config = Self::twap(
            venue, market_id, token_id, side, outcome,
            quantity, limit_price, duration, num_slices
        );
        config.algo_type = AlgoType::VWAP;
        config
    }

    /// Create a POV configuration
    pub fn pov(
        venue: Venue,
        market_id: String,
        token_id: String,
        side: Side,
        outcome: Outcome,
        quantity: Quantity,
        limit_price: Price,
        duration: Duration,
        participation_rate: Decimal,
    ) -> Self {
        let now = Utc::now();
        Self {
            algo_type: AlgoType::POV,
            venue,
            market_id,
            token_id,
            side,
            outcome,
            total_quantity: quantity,
            limit_price,
            start_time: now,
            end_time: now + duration,
            num_slices: 0,  // POV determines dynamically
            target_participation_rate: participation_rate,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_volume_profile_share() {
        let mut profile = VolumeProfile::default();
        // Set non-uniform profile
        profile.hourly_volumes[9] = dec!(0.10);   // 10% at 9am
        profile.hourly_volumes[10] = dec!(0.15);  // 15% at 10am
        profile.hourly_volumes[14] = dec!(0.12);  // 12% at 2pm

        assert_eq!(profile.volume_share_for_hour(9), dec!(0.10));
        assert_eq!(profile.volume_share_for_hour(10), dec!(0.15));
        assert_eq!(profile.volume_share_for_hour(25), dec!(0));  // Invalid hour
    }

    #[test]
    fn test_algo_config_builders() {
        let config = AlgoConfig::twap(
            Venue::Polymarket,
            "market1".to_string(),
            "token1".to_string(),
            Side::Buy,
            Outcome::Yes,
            Quantity::new(dec!(1000)).unwrap(),
            Price::new(dec!(0.55)).unwrap(),
            Duration::minutes(30),
            10,
        );

        assert_eq!(config.algo_type, AlgoType::TWAP);
        assert_eq!(config.num_slices, 10);
        assert_eq!(config.total_quantity.as_decimal(), dec!(1000));

        let vwap_config = AlgoConfig::vwap(
            Venue::Kalshi,
            "market2".to_string(),
            "token2".to_string(),
            Side::Sell,
            Outcome::No,
            Quantity::new(dec!(500)).unwrap(),
            Price::new(dec!(0.45)).unwrap(),
            Duration::hours(1),
            20,
        );

        assert_eq!(vwap_config.algo_type, AlgoType::VWAP);
        assert_eq!(vwap_config.num_slices, 20);
    }

    #[test]
    fn test_slice_status() {
        let slice = AlgoSlice {
            slice_id: 0,
            target_quantity: dec!(100),
            filled_quantity: dec!(95),
            target_time: Utc::now(),
            executed_at: Some(Utc::now()),
            fill_price: Some(dec!(0.55)),
            order_id: Some("order123".to_string()),
            status: SliceStatus::Filled,
        };

        assert_eq!(slice.status, SliceStatus::Filled);
        assert_eq!(slice.filled_quantity, dec!(95));
    }
}
