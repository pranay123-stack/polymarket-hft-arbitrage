//! Risk limits configuration and validation.

use crate::core::config::RiskConfig;
use rust_decimal::Decimal;

/// Risk limits derived from configuration
#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_position_size: Decimal,
    pub max_total_exposure: Decimal,
    pub max_daily_loss: Decimal,
    pub max_drawdown_pct: Decimal,
    pub max_open_positions: u32,
    pub max_orders_per_minute: u32,
    pub max_correlation: Decimal,
    pub max_consecutive_losses: u32,
}

impl RiskLimits {
    /// Create from configuration
    pub fn from_config(config: &RiskConfig) -> Self {
        Self {
            max_position_size: config.max_position_size,
            max_total_exposure: config.max_total_exposure,
            max_daily_loss: config.max_daily_loss,
            max_drawdown_pct: config.max_drawdown_pct,
            max_open_positions: config.max_open_positions,
            max_orders_per_minute: config.max_orders_per_minute,
            max_correlation: config.max_correlation,
            max_consecutive_losses: config.max_consecutive_losses,
        }
    }

    /// Check if position size is within limits
    pub fn check_position_size(&self, size: Decimal) -> bool {
        size <= self.max_position_size
    }

    /// Check if total exposure is within limits
    pub fn check_total_exposure(&self, exposure: Decimal) -> bool {
        exposure <= self.max_total_exposure
    }

    /// Check if daily loss is within limits
    pub fn check_daily_loss(&self, loss: Decimal) -> bool {
        loss <= self.max_daily_loss
    }

    /// Check if drawdown is within limits
    pub fn check_drawdown(&self, drawdown_pct: Decimal) -> bool {
        drawdown_pct <= self.max_drawdown_pct
    }

    /// Check if number of positions is within limits
    pub fn check_position_count(&self, count: u32) -> bool {
        count < self.max_open_positions
    }

    /// Check if order rate is within limits
    pub fn check_order_rate(&self, rate: u32) -> bool {
        rate < self.max_orders_per_minute
    }
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_position_size: Decimal::from(500),
            max_total_exposure: Decimal::from(2000),
            max_daily_loss: Decimal::from(100),
            max_drawdown_pct: Decimal::from(10),
            max_open_positions: 10,
            max_orders_per_minute: 60,
            max_correlation: Decimal::from_str_exact("0.8").unwrap(),
            max_consecutive_losses: 5,
        }
    }
}
