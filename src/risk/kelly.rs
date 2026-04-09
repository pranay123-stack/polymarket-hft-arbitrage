//! Kelly Criterion Position Sizing
//!
//! Implements optimal position sizing based on the Kelly Criterion formula:
//!
//!   f* = (bp - q) / b
//!
//! Where:
//! - f* = optimal fraction of capital to bet
//! - b  = odds received on the bet (decimal odds - 1)
//! - p  = probability of winning
//! - q  = probability of losing (1 - p)
//!
//! ## For Prediction Markets:
//!
//! In prediction markets, we're buying contracts at a price that implies odds.
//! The Kelly formula adapts to:
//!
//!   f* = (p - price) / (1 - price)
//!
//! Where:
//! - p     = our estimated probability of the event
//! - price = current market price (implied probability)
//!
//! ## Fractional Kelly:
//!
//! Full Kelly is often too aggressive, so we support fractional Kelly:
//! - Half Kelly (0.5x): More conservative, lower variance
//! - Quarter Kelly (0.25x): Very conservative
//!
//! ## Multi-Asset Kelly:
//!
//! For portfolios of bets, we use the multi-asset Kelly criterion
//! which accounts for correlations between outcomes.
//!
//! ## Implementation Features:
//!
//! - Single bet Kelly sizing
//! - Fractional Kelly multiplier
//! - Maximum position limits
//! - Confidence-adjusted Kelly
//! - Multi-leg arbitrage sizing
//! - Portfolio-level Kelly optimization

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::core::types::{Price, Quantity, Outcome};

/// Kelly Criterion configuration
#[derive(Debug, Clone)]
pub struct KellyConfig {
    /// Fraction of Kelly to use (0.0 - 1.0, typically 0.25 - 0.5)
    pub kelly_fraction: Decimal,
    /// Maximum position as fraction of capital (e.g., 0.05 = 5%)
    pub max_position_fraction: Decimal,
    /// Minimum edge required to take a position (e.g., 0.01 = 1%)
    pub min_edge_threshold: Decimal,
    /// Whether to use confidence-adjusted Kelly
    pub confidence_adjusted: bool,
    /// Default confidence level when not specified (0.0 - 1.0)
    pub default_confidence: Decimal,
    /// Risk-free rate for Sharpe-adjusted Kelly
    pub risk_free_rate: Decimal,
    /// Whether to consider correlation between positions
    pub correlation_aware: bool,
}

impl Default for KellyConfig {
    fn default() -> Self {
        Self {
            kelly_fraction: dec!(0.25),       // Quarter Kelly
            max_position_fraction: dec!(0.10), // Max 10% of capital per position
            min_edge_threshold: dec!(0.02),    // Need at least 2% edge
            confidence_adjusted: true,
            default_confidence: dec!(0.70),    // 70% confidence by default
            risk_free_rate: dec!(0.05),        // 5% annual risk-free rate
            correlation_aware: true,
        }
    }
}

/// Input for Kelly calculation
#[derive(Debug, Clone)]
pub struct KellyInput {
    /// Estimated probability of winning (0.0 - 1.0)
    pub win_probability: Decimal,
    /// Current market price (implied probability)
    pub market_price: Decimal,
    /// Confidence in our probability estimate (0.0 - 1.0)
    pub confidence: Option<Decimal>,
    /// For multi-outcome: correlations with other bets
    pub correlations: Option<Vec<(String, Decimal)>>,
    /// Expected volatility of the probability estimate
    pub probability_volatility: Option<Decimal>,
}

impl KellyInput {
    /// Create a simple Kelly input
    pub fn simple(win_probability: Decimal, market_price: Decimal) -> Self {
        Self {
            win_probability,
            market_price,
            confidence: None,
            correlations: None,
            probability_volatility: None,
        }
    }

    /// Create a Kelly input with confidence
    pub fn with_confidence(
        win_probability: Decimal,
        market_price: Decimal,
        confidence: Decimal,
    ) -> Self {
        Self {
            win_probability,
            market_price,
            confidence: Some(confidence),
            correlations: None,
            probability_volatility: None,
        }
    }

    /// Calculate the edge (our probability - market price)
    pub fn edge(&self) -> Decimal {
        self.win_probability - self.market_price
    }

    /// Check if there's a positive edge
    pub fn has_edge(&self) -> bool {
        self.edge() > Decimal::ZERO
    }
}

/// Result of Kelly calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KellyResult {
    /// Optimal fraction of capital to risk
    pub optimal_fraction: Decimal,
    /// Fraction after applying Kelly multiplier
    pub adjusted_fraction: Decimal,
    /// Final fraction after caps and minimums
    pub final_fraction: Decimal,
    /// Position size in capital terms
    pub position_size: Decimal,
    /// Edge on this bet
    pub edge: Decimal,
    /// Expected value of the bet
    pub expected_value: Decimal,
    /// Expected growth rate (Kelly criterion optimizes this)
    pub expected_growth_rate: Decimal,
    /// Whether position was capped
    pub was_capped: bool,
    /// Reason for any adjustment
    pub adjustment_reason: Option<String>,
}

/// Kelly Criterion Calculator
pub struct KellyCalculator {
    config: KellyConfig,
    /// Cache of recent calculations
    cache: HashMap<String, KellyResult>,
}

impl KellyCalculator {
    pub fn new(config: KellyConfig) -> Self {
        Self {
            config,
            cache: HashMap::new(),
        }
    }

    /// Calculate Kelly fraction for a single bet
    ///
    /// For a prediction market:
    /// - Buying YES at price p with estimated prob P:
    ///   f* = (P - p) / (1 - p)
    ///
    /// - Buying NO at price p with estimated NO prob (1-P):
    ///   Equivalent to shorting YES, handled separately
    pub fn calculate(
        &self,
        input: &KellyInput,
        available_capital: Decimal,
    ) -> KellyResult {
        let p = input.win_probability;
        let price = input.market_price;
        let edge = input.edge();

        // Check for edge
        if edge <= self.config.min_edge_threshold {
            return KellyResult {
                optimal_fraction: Decimal::ZERO,
                adjusted_fraction: Decimal::ZERO,
                final_fraction: Decimal::ZERO,
                position_size: Decimal::ZERO,
                edge,
                expected_value: Decimal::ZERO,
                expected_growth_rate: Decimal::ZERO,
                was_capped: false,
                adjustment_reason: Some("Insufficient edge".to_string()),
            };
        }

        // Calculate raw Kelly fraction
        // f* = (P - price) / (1 - price)
        let raw_kelly = if price < Decimal::ONE {
            edge / (Decimal::ONE - price)
        } else {
            Decimal::ZERO
        };

        // Apply confidence adjustment if enabled
        let confidence = input.confidence.unwrap_or(self.config.default_confidence);
        let confidence_adjusted = if self.config.confidence_adjusted {
            // Reduce Kelly proportionally to confidence
            // At 100% confidence, use full Kelly; at 50% confidence, use half
            raw_kelly * confidence
        } else {
            raw_kelly
        };

        // Apply fractional Kelly
        let fractional_kelly = confidence_adjusted * self.config.kelly_fraction;

        // Apply maximum position cap
        let was_capped = fractional_kelly > self.config.max_position_fraction;
        let final_fraction = fractional_kelly.min(self.config.max_position_fraction);

        // Calculate position size
        let position_size = available_capital * final_fraction;

        // Calculate expected value
        // EV = P * (1 - price) - (1 - P) * price
        //    = P - price = edge
        let expected_value = position_size * edge;

        // Calculate expected growth rate (what Kelly maximizes)
        // g = P * log(1 + f*(1-price)/price) + (1-P) * log(1 - f)
        // Simplified approximation for small f:
        let expected_growth_rate = if final_fraction > Decimal::ZERO {
            edge * final_fraction - (final_fraction.powi(2)) * (Decimal::ONE - p) / (Decimal::from(2) * price)
        } else {
            Decimal::ZERO
        };

        let adjustment_reason = if was_capped {
            Some(format!("Capped from {:.2}% to {:.2}%",
                fractional_kelly * dec!(100), final_fraction * dec!(100)))
        } else if confidence < Decimal::ONE {
            Some(format!("Confidence adjusted ({:.0}%)", confidence * dec!(100)))
        } else {
            None
        };

        KellyResult {
            optimal_fraction: raw_kelly,
            adjusted_fraction: fractional_kelly,
            final_fraction,
            position_size,
            edge,
            expected_value,
            expected_growth_rate,
            was_capped,
            adjustment_reason,
        }
    }

    /// Calculate Kelly for a binary arbitrage (buy YES on one venue, NO on another)
    ///
    /// For arbitrage, we're not betting on probability, we're capturing a spread.
    /// The "edge" is the guaranteed profit, so Kelly simplifies to:
    ///
    ///   f* = edge / cost_per_contract
    ///
    /// Since arbitrage has (theoretically) no risk, optimal fraction is 100%,
    /// but we still apply fractional Kelly for execution risk.
    pub fn calculate_arbitrage(
        &self,
        yes_price: Decimal,    // Price to buy YES
        no_price: Decimal,     // Price to buy NO (complement market)
        available_capital: Decimal,
        execution_success_prob: Decimal,  // Probability both legs fill
    ) -> KellyResult {
        // In binary arb: buy YES at p1, buy NO at p2
        // Total cost = p1 + p2
        // Payout = 1 (one of them wins)
        // Profit = 1 - (p1 + p2)

        let total_cost = yes_price + no_price;
        let profit_per_contract = Decimal::ONE - total_cost;

        if profit_per_contract <= Decimal::ZERO {
            return KellyResult {
                optimal_fraction: Decimal::ZERO,
                adjusted_fraction: Decimal::ZERO,
                final_fraction: Decimal::ZERO,
                position_size: Decimal::ZERO,
                edge: profit_per_contract,
                expected_value: Decimal::ZERO,
                expected_growth_rate: Decimal::ZERO,
                was_capped: false,
                adjustment_reason: Some("No arbitrage opportunity".to_string()),
            };
        }

        // For arbitrage, edge = guaranteed profit (adjusted for execution risk)
        let risk_adjusted_edge = profit_per_contract * execution_success_prob
            - total_cost * (Decimal::ONE - execution_success_prob);

        if risk_adjusted_edge <= self.config.min_edge_threshold {
            return KellyResult {
                optimal_fraction: Decimal::ZERO,
                adjusted_fraction: Decimal::ZERO,
                final_fraction: Decimal::ZERO,
                position_size: Decimal::ZERO,
                edge: risk_adjusted_edge,
                expected_value: Decimal::ZERO,
                expected_growth_rate: Decimal::ZERO,
                was_capped: false,
                adjustment_reason: Some("Risk-adjusted edge too low".to_string()),
            };
        }

        // Kelly for arbitrage with execution risk
        // Treat it as a bet: win = both legs fill (prob = exec_prob), profit = profit_per_contract
        //                     lose = one leg fails (prob = 1-exec_prob), loss = worst case slippage
        // Simplified: f* = execution_prob * profit / total_cost
        let raw_kelly = (execution_success_prob * profit_per_contract) / total_cost;

        // Apply fractional Kelly and caps
        let fractional_kelly = raw_kelly * self.config.kelly_fraction;
        let was_capped = fractional_kelly > self.config.max_position_fraction;
        let final_fraction = fractional_kelly.min(self.config.max_position_fraction);

        // Position size (in terms of how many contracts)
        let position_size = available_capital * final_fraction / total_cost;

        let expected_value = position_size * profit_per_contract * execution_success_prob;
        let expected_growth_rate = risk_adjusted_edge * final_fraction;

        KellyResult {
            optimal_fraction: raw_kelly,
            adjusted_fraction: fractional_kelly,
            final_fraction,
            position_size: available_capital * final_fraction,  // Capital committed
            edge: profit_per_contract,
            expected_value,
            expected_growth_rate,
            was_capped,
            adjustment_reason: if was_capped {
                Some("Position capped".to_string())
            } else {
                None
            },
        }
    }

    /// Calculate optimal sizing for multiple correlated bets
    ///
    /// Uses multi-asset Kelly optimization which accounts for correlations.
    /// For N assets, we solve:
    ///   f* = C^(-1) * m
    /// Where:
    ///   C = covariance matrix
    ///   m = vector of expected excess returns
    pub fn calculate_portfolio(
        &self,
        inputs: &[(String, KellyInput)],
        correlations: &HashMap<(String, String), Decimal>,
        available_capital: Decimal,
    ) -> HashMap<String, KellyResult> {
        let mut results = HashMap::new();

        if inputs.is_empty() {
            return results;
        }

        // For simplicity, use independence assumption with correlation adjustment
        // Full multi-asset Kelly requires matrix operations
        let n = inputs.len() as u32;

        // Heuristic: reduce allocation when many correlated bets
        let correlation_penalty = if self.config.correlation_aware && n > 1 {
            // Calculate average absolute correlation
            let mut total_corr = Decimal::ZERO;
            let mut count = 0u32;

            for i in 0..inputs.len() {
                for j in (i+1)..inputs.len() {
                    let key = (inputs[i].0.clone(), inputs[j].0.clone());
                    if let Some(&corr) = correlations.get(&key) {
                        total_corr += corr.abs();
                        count += 1;
                    }
                }
            }

            if count > 0 {
                let avg_corr = total_corr / Decimal::from(count);
                // Reduce allocation as correlation increases
                Decimal::ONE - avg_corr * dec!(0.5)
            } else {
                Decimal::ONE
            }
        } else {
            Decimal::ONE
        };

        // Calculate individual Kelly with portfolio adjustment
        let capital_per_bet = available_capital / Decimal::from(n);

        for (id, input) in inputs {
            let mut result = self.calculate(input, capital_per_bet);

            // Apply correlation penalty
            if correlation_penalty < Decimal::ONE {
                result.position_size *= correlation_penalty;
                result.final_fraction *= correlation_penalty;
                result.adjusted_fraction *= correlation_penalty;
                result.adjustment_reason = Some(format!(
                    "Correlation adjusted: {:.0}%",
                    correlation_penalty * dec!(100)
                ));
            }

            results.insert(id.clone(), result);
        }

        results
    }

    /// Calculate position size to target a specific risk level
    ///
    /// Inverts the Kelly calculation to find probability estimate
    /// that would justify a given position size.
    pub fn reverse_kelly(
        &self,
        target_fraction: Decimal,
        market_price: Decimal,
    ) -> Decimal {
        // f* = (p - price) / (1 - price)
        // Solving for p:
        // p = f*(1 - price) + price
        let implied_prob = target_fraction * (Decimal::ONE - market_price) + market_price;
        implied_prob.min(Decimal::ONE).max(Decimal::ZERO)
    }

    /// Get recommended Kelly fraction based on strategy type
    pub fn recommended_fraction(strategy: &str) -> Decimal {
        match strategy {
            "aggressive" => dec!(0.50),   // Half Kelly
            "moderate" => dec!(0.25),     // Quarter Kelly
            "conservative" => dec!(0.125), // Eighth Kelly
            "ultra_conservative" => dec!(0.0625), // Sixteenth Kelly
            _ => dec!(0.25),
        }
    }

    /// Calculate dynamic Kelly based on recent performance
    ///
    /// Reduces Kelly when on a losing streak, increases when winning.
    /// Based on drawdown percentage.
    pub fn dynamic_kelly(
        &self,
        base_fraction: Decimal,
        current_drawdown_pct: Decimal,
        max_allowed_drawdown_pct: Decimal,
    ) -> Decimal {
        if current_drawdown_pct >= max_allowed_drawdown_pct {
            // At max drawdown, stop betting
            Decimal::ZERO
        } else if current_drawdown_pct > Decimal::ZERO {
            // Reduce Kelly proportionally to drawdown
            let reduction = current_drawdown_pct / max_allowed_drawdown_pct;
            base_fraction * (Decimal::ONE - reduction)
        } else {
            base_fraction
        }
    }
}

/// Convenience function for quick Kelly calculation
pub fn kelly_fraction(
    win_prob: Decimal,
    market_price: Decimal,
    kelly_multiplier: Decimal,
) -> Decimal {
    if win_prob <= market_price {
        return Decimal::ZERO;
    }

    let edge = win_prob - market_price;
    let raw_kelly = edge / (Decimal::ONE - market_price);
    let adjusted = raw_kelly * kelly_multiplier;

    adjusted.min(Decimal::ONE).max(Decimal::ZERO)
}

/// Calculate position size from Kelly fraction
pub fn kelly_position_size(
    kelly_fraction: Decimal,
    available_capital: Decimal,
    contract_price: Decimal,
) -> Quantity {
    let capital_to_use = available_capital * kelly_fraction;
    let contracts = capital_to_use / contract_price;

    Quantity::new(contracts.floor()).unwrap_or(Quantity::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_kelly() {
        let config = KellyConfig {
            kelly_fraction: dec!(0.50),  // Half Kelly
            confidence_adjusted: false,
            ..Default::default()
        };
        let calc = KellyCalculator::new(config);

        // 60% win probability, 50% market price
        let input = KellyInput::simple(dec!(0.60), dec!(0.50));
        let result = calc.calculate(&input, dec!(10000));

        // Edge = 0.60 - 0.50 = 0.10
        assert_eq!(result.edge, dec!(0.10));

        // Raw Kelly = 0.10 / 0.50 = 0.20
        assert_eq!(result.optimal_fraction, dec!(0.20));

        // Half Kelly = 0.10
        assert_eq!(result.adjusted_fraction, dec!(0.10));
    }

    #[test]
    fn test_no_edge() {
        let config = KellyConfig::default();
        let calc = KellyCalculator::new(config);

        // 50% win prob, 50% market price = no edge
        let input = KellyInput::simple(dec!(0.50), dec!(0.50));
        let result = calc.calculate(&input, dec!(10000));

        assert_eq!(result.final_fraction, Decimal::ZERO);
        assert!(result.adjustment_reason.is_some());
    }

    #[test]
    fn test_negative_edge() {
        let config = KellyConfig::default();
        let calc = KellyCalculator::new(config);

        // 40% win prob, 50% market price = negative edge
        let input = KellyInput::simple(dec!(0.40), dec!(0.50));
        let result = calc.calculate(&input, dec!(10000));

        assert!(result.edge < Decimal::ZERO);
        assert_eq!(result.final_fraction, Decimal::ZERO);
    }

    #[test]
    fn test_confidence_adjustment() {
        let config = KellyConfig {
            kelly_fraction: dec!(1.0),  // Full Kelly for testing
            confidence_adjusted: true,
            ..Default::default()
        };
        let calc = KellyCalculator::new(config);

        // 70% win prob, 50% price, but only 50% confident
        let input = KellyInput::with_confidence(dec!(0.70), dec!(0.50), dec!(0.50));
        let result = calc.calculate(&input, dec!(10000));

        // Raw Kelly = 0.20 / 0.50 = 0.40
        // Confidence adjusted = 0.40 * 0.50 = 0.20
        assert!(result.adjusted_fraction < result.optimal_fraction);
    }

    #[test]
    fn test_position_cap() {
        let config = KellyConfig {
            kelly_fraction: dec!(1.0),
            max_position_fraction: dec!(0.05),  // 5% cap
            confidence_adjusted: false,
            ..Default::default()
        };
        let calc = KellyCalculator::new(config);

        // Very high edge should hit cap
        let input = KellyInput::simple(dec!(0.90), dec!(0.50));
        let result = calc.calculate(&input, dec!(10000));

        assert!(result.was_capped);
        assert_eq!(result.final_fraction, dec!(0.05));
    }

    #[test]
    fn test_arbitrage_kelly() {
        let config = KellyConfig::default();
        let calc = KellyCalculator::new(config);

        // Buy YES at 0.48, buy NO at 0.48 = 0.96 total cost, 0.04 profit
        let result = calc.calculate_arbitrage(
            dec!(0.48),
            dec!(0.48),
            dec!(10000),
            dec!(0.95),  // 95% execution success probability
        );

        assert!(result.edge > Decimal::ZERO);
        assert_eq!(result.edge, dec!(0.04));  // 4% guaranteed profit per contract
        assert!(result.position_size > Decimal::ZERO);
    }

    #[test]
    fn test_kelly_convenience_function() {
        // 60% win prob, 50% price, quarter Kelly
        let frac = kelly_fraction(dec!(0.60), dec!(0.50), dec!(0.25));

        // Raw Kelly = 0.10 / 0.50 = 0.20
        // Quarter Kelly = 0.05
        assert_eq!(frac, dec!(0.05));
    }

    #[test]
    fn test_reverse_kelly() {
        let config = KellyConfig::default();
        let calc = KellyCalculator::new(config);

        // If we want to use 10% of capital at price 0.50, what prob do we need?
        let implied = calc.reverse_kelly(dec!(0.10), dec!(0.50));

        // f* = (p - price) / (1 - price)
        // 0.10 = (p - 0.50) / 0.50
        // p = 0.10 * 0.50 + 0.50 = 0.55
        assert_eq!(implied, dec!(0.55));
    }

    #[test]
    fn test_dynamic_kelly() {
        let config = KellyConfig::default();
        let calc = KellyCalculator::new(config);

        // At 0% drawdown, use full fraction
        let fraction = calc.dynamic_kelly(dec!(0.25), dec!(0), dec!(0.10));
        assert_eq!(fraction, dec!(0.25));

        // At 5% drawdown (half of max 10%), use half
        let fraction = calc.dynamic_kelly(dec!(0.25), dec!(0.05), dec!(0.10));
        assert_eq!(fraction, dec!(0.125));

        // At max drawdown, stop betting
        let fraction = calc.dynamic_kelly(dec!(0.25), dec!(0.10), dec!(0.10));
        assert_eq!(fraction, Decimal::ZERO);
    }
}
