//! Main risk manager coordinating all risk controls.

use crate::core::config::RiskConfig;
use crate::core::error::{Error, Result, RiskError};
use crate::core::events::{AlertLevel, Event, EventBus, RiskAlertEvent, RiskAlertType};
use crate::core::types::*;
use crate::risk::circuit_breaker::CircuitBreaker;
use crate::risk::limits::RiskLimits;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Main risk manager
pub struct RiskManager {
    config: RiskConfig,
    event_bus: EventBus,
    limits: RiskLimits,
    circuit_breaker: CircuitBreaker,

    // Position tracking
    positions: DashMap<TokenId, Position>,

    // Daily P&L tracking
    daily_pnl: RwLock<DailyPnl>,

    // Order rate tracking
    orders_this_minute: AtomicU32,
    minute_start: RwLock<DateTime<Utc>>,

    // Consecutive loss tracking
    consecutive_losses: AtomicU32,

    // Peak equity for drawdown calculation
    peak_equity: RwLock<Decimal>,
    current_equity: RwLock<Decimal>,

    // Kill switch
    kill_switch_active: std::sync::atomic::AtomicBool,
}

impl RiskManager {
    /// Create a new risk manager
    pub fn new(config: RiskConfig, event_bus: EventBus) -> Self {
        let limits = RiskLimits::from_config(&config);
        let circuit_breaker = CircuitBreaker::new(config.circuit_breaker_cooldown);

        Self {
            config,
            event_bus,
            limits,
            circuit_breaker,
            positions: DashMap::new(),
            daily_pnl: RwLock::new(DailyPnl::new()),
            orders_this_minute: AtomicU32::new(0),
            minute_start: RwLock::new(Utc::now()),
            consecutive_losses: AtomicU32::new(0),
            peak_equity: RwLock::new(Decimal::ZERO),
            current_equity: RwLock::new(Decimal::ZERO),
            kill_switch_active: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Initialize with starting capital
    pub fn initialize(&self, starting_capital: Decimal) {
        let mut peak = self.peak_equity.write();
        let mut current = self.current_equity.write();
        *peak = starting_capital;
        *current = starting_capital;

        info!("Risk manager initialized with capital: {}", starting_capital);
    }

    /// Check if trading is allowed
    pub fn is_trading_allowed(&self) -> bool {
        !self.kill_switch_active.load(Ordering::Relaxed)
            && !self.circuit_breaker.is_open()
    }

    /// Activate kill switch
    pub fn activate_kill_switch(&self) {
        self.kill_switch_active.store(true, Ordering::Relaxed);
        warn!("KILL SWITCH ACTIVATED");

        self.event_bus.publish(Event::alert(
            AlertLevel::Critical,
            "Kill switch activated".to_string(),
            None,
        ));
    }

    /// Deactivate kill switch
    pub fn deactivate_kill_switch(&self) {
        self.kill_switch_active.store(false, Ordering::Relaxed);
        info!("Kill switch deactivated");
    }

    /// Check if an order passes risk limits
    pub fn check_order(&self, order: &Order) -> Result<()> {
        // Check kill switch
        if self.kill_switch_active.load(Ordering::Relaxed) {
            return Err(Error::KillSwitchActivated);
        }

        // Check circuit breaker
        if self.circuit_breaker.is_open() {
            return Err(Error::CircuitBreakerOpen);
        }

        // Check order rate limit
        self.check_order_rate()?;

        // Check position size limit
        let position_value = order.notional();
        if position_value > self.config.max_position_size {
            self.emit_risk_alert(
                RiskAlertType::PositionSize,
                &format!("Order size {} exceeds limit {}", position_value, self.config.max_position_size),
                position_value,
                self.config.max_position_size,
            );
            return Err(Error::Risk(RiskError::PositionSizeExceeded {
                current: position_value.to_string(),
                limit: self.config.max_position_size.to_string(),
            }));
        }

        // Check total exposure
        let current_exposure = self.calculate_total_exposure();
        let new_exposure = current_exposure + position_value;

        if new_exposure > self.config.max_total_exposure {
            self.emit_risk_alert(
                RiskAlertType::Exposure,
                &format!("Total exposure {} exceeds limit {}", new_exposure, self.config.max_total_exposure),
                new_exposure,
                self.config.max_total_exposure,
            );
            return Err(Error::Risk(RiskError::ExposureLimitExceeded {
                current: new_exposure.to_string(),
                limit: self.config.max_total_exposure.to_string(),
            }));
        }

        // Check number of open positions
        let open_positions = self.positions.len() as u32;
        if open_positions >= self.config.max_open_positions {
            return Err(Error::Risk(RiskError::TooManyPositions {
                current: open_positions,
                limit: self.config.max_open_positions,
            }));
        }

        // Increment order counter
        self.orders_this_minute.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Check if an arbitrage opportunity passes risk limits
    pub fn check_arbitrage(&self, opportunity: &ArbitrageOpportunity) -> Result<()> {
        // Check kill switch
        if self.kill_switch_active.load(Ordering::Relaxed) {
            return Err(Error::KillSwitchActivated);
        }

        // Check circuit breaker
        if self.circuit_breaker.is_open() {
            return Err(Error::CircuitBreakerOpen);
        }

        // Check required capital against exposure limits
        if opportunity.required_capital > self.config.max_total_exposure {
            return Err(Error::Risk(RiskError::ExposureLimitExceeded {
                current: opportunity.required_capital.to_string(),
                limit: self.config.max_total_exposure.to_string(),
            }));
        }

        // Check daily loss limit
        let daily_loss = self.daily_pnl.read().unrealized_loss();
        if daily_loss >= self.config.max_daily_loss {
            self.emit_risk_alert(
                RiskAlertType::DailyLoss,
                &format!("Daily loss {} exceeds limit {}", daily_loss, self.config.max_daily_loss),
                daily_loss,
                self.config.max_daily_loss,
            );
            return Err(Error::Risk(RiskError::DailyLossExceeded {
                current: daily_loss.to_string(),
                limit: self.config.max_daily_loss.to_string(),
            }));
        }

        // Check drawdown
        let current_drawdown = self.calculate_drawdown();
        if current_drawdown >= self.config.max_drawdown_pct {
            self.emit_risk_alert(
                RiskAlertType::Drawdown,
                &format!("Drawdown {}% exceeds limit {}%", current_drawdown, self.config.max_drawdown_pct),
                current_drawdown,
                self.config.max_drawdown_pct,
            );
            return Err(Error::Risk(RiskError::DrawdownExceeded {
                current_pct: current_drawdown.to_string(),
                limit_pct: self.config.max_drawdown_pct.to_string(),
            }));
        }

        // Check confidence/risk score
        if opportunity.risk_score > 0.8 {
            debug!(
                "High risk opportunity rejected: score {}",
                opportunity.risk_score
            );
            return Err(Error::Risk(RiskError::HighVolatility));
        }

        Ok(())
    }

    /// Record a trade for P&L tracking
    pub fn record_trade(&self, trade: &Trade) {
        // Update position
        let position = self.positions
            .entry(trade.token_id.clone())
            .or_insert_with(|| Position::new(
                trade.market_id.clone(),
                trade.token_id.clone(),
                trade.outcome,
            ));

        position.apply_trade(trade);

        // Update daily P&L
        {
            let mut daily_pnl = self.daily_pnl.write();
            daily_pnl.add_trade(trade);
        }

        // Update equity tracking
        self.update_equity();

        // Track consecutive losses
        if trade.net_amount() < Decimal::ZERO {
            let losses = self.consecutive_losses.fetch_add(1, Ordering::Relaxed) + 1;
            if losses >= self.config.max_consecutive_losses {
                warn!("Consecutive losses exceeded: {}", losses);
                self.circuit_breaker.trip("Consecutive losses exceeded");

                self.event_bus.publish(Event::new(
                    crate::core::events::EventPayload::CircuitBreakerTriggered(
                        crate::core::events::CircuitBreakerEvent {
                            reason: format!("Consecutive losses: {}", losses),
                            cooldown_until: Utc::now() + self.config.circuit_breaker_cooldown,
                            timestamp: Utc::now(),
                        }
                    )
                ));
            }
        } else {
            self.consecutive_losses.store(0, Ordering::Relaxed);
        }
    }

    /// Check order rate limit
    fn check_order_rate(&self) -> Result<()> {
        let now = Utc::now();
        let minute_start = *self.minute_start.read();

        // Reset counter if new minute
        if now - minute_start > Duration::minutes(1) {
            self.orders_this_minute.store(0, Ordering::Relaxed);
            *self.minute_start.write() = now;
        }

        let current = self.orders_this_minute.load(Ordering::Relaxed);
        if current >= self.config.max_orders_per_minute {
            return Err(Error::Risk(RiskError::OrderRateLimitExceeded {
                current,
                limit: self.config.max_orders_per_minute,
            }));
        }

        Ok(())
    }

    /// Calculate total exposure across all positions
    pub fn calculate_total_exposure(&self) -> Decimal {
        self.positions
            .iter()
            .map(|entry| entry.value().notional())
            .sum()
    }

    /// Calculate current drawdown percentage
    pub fn calculate_drawdown(&self) -> Decimal {
        let peak = *self.peak_equity.read();
        let current = *self.current_equity.read();

        if peak <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        ((peak - current) / peak) * dec!(100)
    }

    /// Update equity tracking
    fn update_equity(&self) {
        let total_value = self.calculate_total_exposure();
        let daily_pnl = self.daily_pnl.read().net_pnl();

        let starting = *self.peak_equity.read(); // Approximation
        let new_equity = starting + daily_pnl;

        {
            let mut current = self.current_equity.write();
            *current = new_equity;
        }

        // Update peak if new high
        if new_equity > *self.peak_equity.read() {
            let mut peak = self.peak_equity.write();
            *peak = new_equity;
        }
    }

    /// Emit a risk alert
    fn emit_risk_alert(
        &self,
        alert_type: RiskAlertType,
        message: &str,
        current: Decimal,
        threshold: Decimal,
    ) {
        let event = Event::new(crate::core::events::EventPayload::RiskAlert(
            RiskAlertEvent {
                alert_type,
                message: message.to_string(),
                current_value: current.to_string(),
                threshold: threshold.to_string(),
                timestamp: Utc::now(),
            }
        ));

        self.event_bus.publish(event);
    }

    /// Get current risk status
    pub fn get_status(&self) -> RiskStatus {
        let daily_pnl = self.daily_pnl.read();

        RiskStatus {
            trading_allowed: self.is_trading_allowed(),
            kill_switch_active: self.kill_switch_active.load(Ordering::Relaxed),
            circuit_breaker_open: self.circuit_breaker.is_open(),
            circuit_breaker_cooldown_remaining: self.circuit_breaker.cooldown_remaining(),
            total_exposure: self.calculate_total_exposure(),
            max_exposure: self.config.max_total_exposure,
            daily_pnl: daily_pnl.net_pnl(),
            daily_loss_limit: self.config.max_daily_loss,
            drawdown_pct: self.calculate_drawdown(),
            max_drawdown_pct: self.config.max_drawdown_pct,
            open_positions: self.positions.len() as u32,
            max_positions: self.config.max_open_positions,
            orders_this_minute: self.orders_this_minute.load(Ordering::Relaxed),
            max_orders_per_minute: self.config.max_orders_per_minute,
            consecutive_losses: self.consecutive_losses.load(Ordering::Relaxed),
        }
    }

    /// Reset daily tracking (call at start of new trading day)
    pub fn reset_daily(&self) {
        *self.daily_pnl.write() = DailyPnl::new();
        self.consecutive_losses.store(0, Ordering::Relaxed);
        self.orders_this_minute.store(0, Ordering::Relaxed);
        *self.minute_start.write() = Utc::now();

        // Reset circuit breaker if needed
        self.circuit_breaker.reset();

        info!("Daily risk tracking reset");
    }
}

/// Daily P&L tracking
#[derive(Debug)]
struct DailyPnl {
    realized_profit: Decimal,
    realized_loss: Decimal,
    unrealized_pnl: Decimal,
    trades: Vec<TradeRecord>,
    start_time: DateTime<Utc>,
}

impl DailyPnl {
    fn new() -> Self {
        Self {
            realized_profit: Decimal::ZERO,
            realized_loss: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            trades: Vec::new(),
            start_time: Utc::now(),
        }
    }

    fn add_trade(&mut self, trade: &Trade) {
        let pnl = trade.net_amount();

        if pnl > Decimal::ZERO {
            self.realized_profit += pnl;
        } else {
            self.realized_loss += pnl.abs();
        }

        self.trades.push(TradeRecord {
            trade_id: trade.id.clone(),
            pnl,
            timestamp: trade.timestamp,
        });
    }

    fn net_pnl(&self) -> Decimal {
        self.realized_profit - self.realized_loss + self.unrealized_pnl
    }

    fn unrealized_loss(&self) -> Decimal {
        if self.unrealized_pnl < Decimal::ZERO {
            self.unrealized_pnl.abs()
        } else {
            Decimal::ZERO
        }
    }
}

#[derive(Debug)]
struct TradeRecord {
    trade_id: String,
    pnl: Decimal,
    timestamp: DateTime<Utc>,
}

/// Current risk status
#[derive(Debug, Clone)]
pub struct RiskStatus {
    pub trading_allowed: bool,
    pub kill_switch_active: bool,
    pub circuit_breaker_open: bool,
    pub circuit_breaker_cooldown_remaining: Option<std::time::Duration>,
    pub total_exposure: Decimal,
    pub max_exposure: Decimal,
    pub daily_pnl: Decimal,
    pub daily_loss_limit: Decimal,
    pub drawdown_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub open_positions: u32,
    pub max_positions: u32,
    pub orders_this_minute: u32,
    pub max_orders_per_minute: u32,
    pub consecutive_losses: u32,
}

impl std::fmt::Debug for RiskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RiskManager")
            .field("trading_allowed", &self.is_trading_allowed())
            .field("positions", &self.positions.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> RiskManager {
        let config = RiskConfig::default();
        let event_bus = EventBus::new(100);
        RiskManager::new(config, event_bus)
    }

    #[test]
    fn test_trading_allowed() {
        let manager = create_test_manager();
        assert!(manager.is_trading_allowed());
    }

    #[test]
    fn test_kill_switch() {
        let manager = create_test_manager();

        manager.activate_kill_switch();
        assert!(!manager.is_trading_allowed());

        manager.deactivate_kill_switch();
        assert!(manager.is_trading_allowed());
    }

    #[test]
    fn test_position_limit() {
        let manager = create_test_manager();
        manager.initialize(dec!(10000));

        // Small order should pass
        let small_order = Order::new(
            "market1".to_string(),
            "token1".to_string(),
            Side::Buy,
            Outcome::Yes,
            Price::new(dec!(0.50)).unwrap(),
            Quantity(dec!(100)),
            OrderType::Gtc,
        );

        assert!(manager.check_order(&small_order).is_ok());

        // Large order should fail
        let large_order = Order::new(
            "market1".to_string(),
            "token1".to_string(),
            Side::Buy,
            Outcome::Yes,
            Price::new(dec!(0.50)).unwrap(),
            Quantity(dec!(100000)),
            OrderType::Gtc,
        );

        assert!(manager.check_order(&large_order).is_err());
    }
}
