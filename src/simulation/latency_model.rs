//! Latency modeling for realistic simulation.
//!
//! Models network latency, exchange processing time, and coordination delays
//! for accurate cross-platform arbitrage backtesting.

use std::collections::HashMap;
use std::time::Duration;
use rand::Rng;
use rand_distr::{Distribution, LogNormal, Normal};
use serde::{Deserialize, Serialize};

/// Venue identifier for latency configuration.
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

/// Latency configuration for a specific venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueLatencyConfig {
    /// Mean network round-trip time in milliseconds.
    pub mean_network_rtt_ms: f64,
    /// Standard deviation of network RTT.
    pub network_rtt_std_ms: f64,
    /// Mean exchange processing time in milliseconds.
    pub mean_exchange_processing_ms: f64,
    /// Standard deviation of exchange processing time.
    pub exchange_processing_std_ms: f64,
    /// Probability of a network spike (0.0 to 1.0).
    pub spike_probability: f64,
    /// Multiplier for latency during spikes.
    pub spike_multiplier: f64,
    /// WebSocket feed delay (time from event to receipt).
    pub ws_feed_delay_ms: f64,
    /// Order acknowledgment delay after submission.
    pub order_ack_delay_ms: f64,
}

impl Default for VenueLatencyConfig {
    fn default() -> Self {
        Self {
            mean_network_rtt_ms: 50.0,
            network_rtt_std_ms: 10.0,
            mean_exchange_processing_ms: 5.0,
            exchange_processing_std_ms: 2.0,
            spike_probability: 0.01,
            spike_multiplier: 5.0,
            ws_feed_delay_ms: 10.0,
            order_ack_delay_ms: 15.0,
        }
    }
}

impl VenueLatencyConfig {
    /// Create configuration for Polymarket (typically faster, blockchain-based).
    pub fn polymarket() -> Self {
        Self {
            mean_network_rtt_ms: 30.0,
            network_rtt_std_ms: 8.0,
            mean_exchange_processing_ms: 50.0, // Blockchain finality
            exchange_processing_std_ms: 20.0,
            spike_probability: 0.02,
            spike_multiplier: 3.0,
            ws_feed_delay_ms: 100.0, // Block time variance
            order_ack_delay_ms: 200.0,
        }
    }

    /// Create configuration for Kalshi (regulated exchange, consistent).
    pub fn kalshi() -> Self {
        Self {
            mean_network_rtt_ms: 25.0,
            network_rtt_std_ms: 5.0,
            mean_exchange_processing_ms: 3.0,
            exchange_processing_std_ms: 1.0,
            spike_probability: 0.005,
            spike_multiplier: 4.0,
            ws_feed_delay_ms: 5.0,
            order_ack_delay_ms: 10.0,
        }
    }

    /// Create configuration for Opinion (Betfair-style).
    pub fn opinion() -> Self {
        Self {
            mean_network_rtt_ms: 35.0,
            network_rtt_std_ms: 10.0,
            mean_exchange_processing_ms: 8.0,
            exchange_processing_std_ms: 3.0,
            spike_probability: 0.01,
            spike_multiplier: 4.0,
            ws_feed_delay_ms: 15.0,
            order_ack_delay_ms: 20.0,
        }
    }
}

/// Latency profile for different market conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LatencyProfile {
    /// Normal market conditions.
    Normal,
    /// High volatility periods (news events, etc.).
    HighVolatility,
    /// Low liquidity periods (off-hours).
    LowLiquidity,
    /// Degraded conditions (partial outages).
    Degraded,
    /// Stress test (worst case).
    StressTest,
}

impl LatencyProfile {
    /// Get multiplier for latency based on profile.
    pub fn latency_multiplier(&self) -> f64 {
        match self {
            LatencyProfile::Normal => 1.0,
            LatencyProfile::HighVolatility => 1.5,
            LatencyProfile::LowLiquidity => 1.2,
            LatencyProfile::Degraded => 3.0,
            LatencyProfile::StressTest => 5.0,
        }
    }

    /// Get multiplier for variance based on profile.
    pub fn variance_multiplier(&self) -> f64 {
        match self {
            LatencyProfile::Normal => 1.0,
            LatencyProfile::HighVolatility => 2.0,
            LatencyProfile::LowLiquidity => 1.5,
            LatencyProfile::Degraded => 4.0,
            LatencyProfile::StressTest => 8.0,
        }
    }
}

/// Latency model for simulating realistic network and exchange delays.
pub struct LatencyModel {
    /// Per-venue latency configurations.
    venue_configs: HashMap<Venue, VenueLatencyConfig>,
    /// Current latency profile.
    profile: LatencyProfile,
    /// Random number generator.
    rng: rand::rngs::ThreadRng,
    /// Cross-venue coordination overhead in milliseconds.
    cross_venue_overhead_ms: f64,
    /// Historical latency samples for analysis.
    latency_history: Vec<LatencySample>,
    /// Maximum history size.
    max_history_size: usize,
}

/// A sample of latency measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySample {
    pub timestamp_ms: u64,
    pub venue: Venue,
    pub latency_type: LatencyType,
    pub latency_ms: f64,
    pub was_spike: bool,
}

/// Type of latency being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LatencyType {
    NetworkRtt,
    ExchangeProcessing,
    WsFeed,
    OrderAck,
    TotalOrderLatency,
    CrossVenueCoordination,
}

impl LatencyModel {
    /// Create a new latency model with default configurations.
    pub fn new() -> Self {
        let mut venue_configs = HashMap::new();
        venue_configs.insert(Venue::Polymarket, VenueLatencyConfig::polymarket());
        venue_configs.insert(Venue::Kalshi, VenueLatencyConfig::kalshi());
        venue_configs.insert(Venue::Opinion, VenueLatencyConfig::opinion());

        Self {
            venue_configs,
            profile: LatencyProfile::Normal,
            rng: rand::thread_rng(),
            cross_venue_overhead_ms: 5.0,
            latency_history: Vec::new(),
            max_history_size: 10000,
        }
    }

    /// Create with custom venue configurations.
    pub fn with_configs(configs: HashMap<Venue, VenueLatencyConfig>) -> Self {
        Self {
            venue_configs: configs,
            profile: LatencyProfile::Normal,
            rng: rand::thread_rng(),
            cross_venue_overhead_ms: 5.0,
            latency_history: Vec::new(),
            max_history_size: 10000,
        }
    }

    /// Set the latency profile.
    pub fn set_profile(&mut self, profile: LatencyProfile) {
        self.profile = profile;
    }

    /// Get network round-trip time for a venue.
    pub fn sample_network_rtt(&mut self, venue: Venue) -> Duration {
        let config = self.venue_configs.get(&venue).cloned().unwrap_or_default();
        let multiplier = self.profile.latency_multiplier();
        let var_mult = self.profile.variance_multiplier();

        let mean = config.mean_network_rtt_ms * multiplier;
        let std = config.network_rtt_std_ms * var_mult;

        let is_spike = self.rng.gen::<f64>() < config.spike_probability;
        let base_latency = self.sample_log_normal(mean, std);

        let latency = if is_spike {
            base_latency * config.spike_multiplier
        } else {
            base_latency
        };

        self.record_sample(venue, LatencyType::NetworkRtt, latency, is_spike);
        Duration::from_micros((latency * 1000.0) as u64)
    }

    /// Get exchange processing time for a venue.
    pub fn sample_exchange_processing(&mut self, venue: Venue) -> Duration {
        let config = self.venue_configs.get(&venue).cloned().unwrap_or_default();
        let multiplier = self.profile.latency_multiplier();
        let var_mult = self.profile.variance_multiplier();

        let mean = config.mean_exchange_processing_ms * multiplier;
        let std = config.exchange_processing_std_ms * var_mult;

        let latency = self.sample_log_normal(mean, std);
        self.record_sample(venue, LatencyType::ExchangeProcessing, latency, false);
        Duration::from_micros((latency * 1000.0) as u64)
    }

    /// Get WebSocket feed delay for a venue.
    pub fn sample_ws_feed_delay(&mut self, venue: Venue) -> Duration {
        let config = self.venue_configs.get(&venue).cloned().unwrap_or_default();
        let multiplier = self.profile.latency_multiplier();

        let mean = config.ws_feed_delay_ms * multiplier;
        let std = mean * 0.3; // 30% variance

        let latency = self.sample_log_normal(mean, std);
        self.record_sample(venue, LatencyType::WsFeed, latency, false);
        Duration::from_micros((latency * 1000.0) as u64)
    }

    /// Get order acknowledgment delay for a venue.
    pub fn sample_order_ack_delay(&mut self, venue: Venue) -> Duration {
        let config = self.venue_configs.get(&venue).cloned().unwrap_or_default();
        let multiplier = self.profile.latency_multiplier();

        let mean = config.order_ack_delay_ms * multiplier;
        let std = mean * 0.25;

        let latency = self.sample_log_normal(mean, std);
        self.record_sample(venue, LatencyType::OrderAck, latency, false);
        Duration::from_micros((latency * 1000.0) as u64)
    }

    /// Get total order latency (network + exchange + ack).
    pub fn sample_total_order_latency(&mut self, venue: Venue) -> Duration {
        let network = self.sample_network_rtt(venue);
        let exchange = self.sample_exchange_processing(venue);
        let ack = self.sample_order_ack_delay(venue);

        let total = network + exchange + ack;
        let total_ms = total.as_secs_f64() * 1000.0;
        self.record_sample(venue, LatencyType::TotalOrderLatency, total_ms, false);

        total
    }

    /// Get cross-venue coordination latency.
    pub fn sample_cross_venue_latency(&mut self, venue_a: Venue, venue_b: Venue) -> Duration {
        let latency_a = self.sample_total_order_latency(venue_a);
        let latency_b = self.sample_total_order_latency(venue_b);

        // Add coordination overhead
        let overhead_ms = self.cross_venue_overhead_ms * self.profile.latency_multiplier();
        let overhead = Duration::from_micros((overhead_ms * 1000.0) as u64);

        let total = latency_a.max(latency_b) + overhead;
        let total_ms = total.as_secs_f64() * 1000.0;

        // Record with the faster venue
        let faster_venue = if latency_a < latency_b { venue_a } else { venue_b };
        self.record_sample(faster_venue, LatencyType::CrossVenueCoordination, total_ms, false);

        total
    }

    /// Sample from a log-normal distribution (positive values, right-skewed).
    fn sample_log_normal(&mut self, mean: f64, std: f64) -> f64 {
        if std <= 0.0 || mean <= 0.0 {
            return mean.max(0.1);
        }

        // Convert mean/std to log-normal parameters
        let variance = std * std;
        let mu = (mean * mean / (mean * mean + variance).sqrt()).ln();
        let sigma = (1.0 + variance / (mean * mean)).ln().sqrt();

        match LogNormal::new(mu, sigma) {
            Ok(dist) => dist.sample(&mut self.rng).max(0.1),
            Err(_) => mean,
        }
    }

    /// Record a latency sample for analysis.
    fn record_sample(&mut self, venue: Venue, latency_type: LatencyType, latency_ms: f64, was_spike: bool) {
        if self.latency_history.len() >= self.max_history_size {
            self.latency_history.remove(0);
        }

        self.latency_history.push(LatencySample {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            venue,
            latency_type,
            latency_ms,
            was_spike,
        });
    }

    /// Get latency statistics for a venue.
    pub fn get_venue_stats(&self, venue: Venue) -> VenueLatencyStats {
        let samples: Vec<_> = self.latency_history
            .iter()
            .filter(|s| s.venue == venue && s.latency_type == LatencyType::TotalOrderLatency)
            .collect();

        if samples.is_empty() {
            return VenueLatencyStats::default();
        }

        let latencies: Vec<f64> = samples.iter().map(|s| s.latency_ms).collect();
        let n = latencies.len() as f64;

        let mean = latencies.iter().sum::<f64>() / n;
        let variance = latencies.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let std_dev = variance.sqrt();

        let mut sorted = latencies.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p50 = percentile(&sorted, 0.5);
        let p95 = percentile(&sorted, 0.95);
        let p99 = percentile(&sorted, 0.99);
        let min = sorted.first().copied().unwrap_or(0.0);
        let max = sorted.last().copied().unwrap_or(0.0);

        let spike_count = samples.iter().filter(|s| s.was_spike).count();
        let spike_rate = spike_count as f64 / samples.len() as f64;

        VenueLatencyStats {
            venue,
            sample_count: samples.len(),
            mean_ms: mean,
            std_dev_ms: std_dev,
            min_ms: min,
            max_ms: max,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            spike_rate,
        }
    }

    /// Get all historical latency samples.
    pub fn get_history(&self) -> &[LatencySample] {
        &self.latency_history
    }

    /// Clear latency history.
    pub fn clear_history(&mut self) {
        self.latency_history.clear();
    }

    /// Estimate probability of successful cross-venue execution within time budget.
    pub fn estimate_success_probability(
        &self,
        venue_a: Venue,
        venue_b: Venue,
        time_budget_ms: f64,
    ) -> f64 {
        let stats_a = self.get_venue_stats(venue_a);
        let stats_b = self.get_venue_stats(venue_b);

        if stats_a.sample_count == 0 || stats_b.sample_count == 0 {
            return 0.5; // Unknown, assume 50%
        }

        // Conservative estimate: use p95 as the typical worst case
        let expected_total = stats_a.p95_ms + stats_b.p95_ms + self.cross_venue_overhead_ms;

        if expected_total <= time_budget_ms * 0.5 {
            0.95 // Very likely to succeed
        } else if expected_total <= time_budget_ms * 0.8 {
            0.80
        } else if expected_total <= time_budget_ms {
            0.60
        } else if expected_total <= time_budget_ms * 1.5 {
            0.30
        } else {
            0.10 // Unlikely to succeed
        }
    }
}

impl Default for LatencyModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for venue latency.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenueLatencyStats {
    pub venue: Venue,
    pub sample_count: usize,
    pub mean_ms: f64,
    pub std_dev_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub spike_rate: f64,
}

impl Default for Venue {
    fn default() -> Self {
        Venue::Polymarket
    }
}

/// Calculate percentile from sorted array.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_model_creation() {
        let model = LatencyModel::new();
        assert!(model.venue_configs.contains_key(&Venue::Polymarket));
        assert!(model.venue_configs.contains_key(&Venue::Kalshi));
        assert!(model.venue_configs.contains_key(&Venue::Opinion));
    }

    #[test]
    fn test_latency_sampling() {
        let mut model = LatencyModel::new();

        for _ in 0..100 {
            let rtt = model.sample_network_rtt(Venue::Kalshi);
            assert!(rtt.as_millis() > 0);
        }

        let stats = model.get_venue_stats(Venue::Kalshi);
        assert!(stats.sample_count > 0);
    }

    #[test]
    fn test_profile_affects_latency() {
        let mut model = LatencyModel::new();

        // Sample in normal conditions
        model.set_profile(LatencyProfile::Normal);
        let normal_samples: Vec<_> = (0..100)
            .map(|_| model.sample_network_rtt(Venue::Kalshi).as_micros())
            .collect();

        model.clear_history();

        // Sample in stress test conditions
        model.set_profile(LatencyProfile::StressTest);
        let stress_samples: Vec<_> = (0..100)
            .map(|_| model.sample_network_rtt(Venue::Kalshi).as_micros())
            .collect();

        let normal_avg: u128 = normal_samples.iter().sum::<u128>() / 100;
        let stress_avg: u128 = stress_samples.iter().sum::<u128>() / 100;

        // Stress should be significantly higher
        assert!(stress_avg > normal_avg * 2);
    }
}
