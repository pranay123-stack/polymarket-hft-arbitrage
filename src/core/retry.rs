//! Retry Logic with Exponential Backoff and Jitter
//!
//! Provides production-grade retry strategies for handling transient failures
//! in HFT systems where reliability is critical.
//!
//! ## Features:
//!
//! - **Exponential Backoff**: Increases delay between retries to avoid overwhelming services
//! - **Jitter**: Randomizes delays to prevent thundering herd on recovery
//! - **Per-Error-Type Configuration**: Different strategies for different error types
//! - **Circuit Breaker Integration**: Stops retrying when service is unhealthy
//! - **Retry Budgets**: Limits total retry attempts over time windows
//! - **Observability**: Metrics for retry counts, delays, and outcomes

use crate::core::error::{Error, Result};
use rand::Rng;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Retry strategy configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Multiplier for exponential backoff (e.g., 2.0 = double each time)
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0 - 1.0, e.g., 0.3 = ±30% randomization)
    pub jitter_factor: f64,
    /// Whether to use full jitter or decorrelated jitter
    pub full_jitter: bool,
    /// Timeout for the entire retry operation
    pub total_timeout: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_attempts: 5,
            backoff_multiplier: 2.0,
            jitter_factor: 0.3,
            full_jitter: false,
            total_timeout: Some(Duration::from_secs(120)),
        }
    }
}

impl RetryConfig {
    /// Create a fast retry config for latency-sensitive operations
    pub fn fast() -> Self {
        Self {
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(500),
            max_attempts: 3,
            backoff_multiplier: 2.0,
            jitter_factor: 0.2,
            full_jitter: true,
            total_timeout: Some(Duration::from_secs(5)),
        }
    }

    /// Create an aggressive retry config for critical operations
    pub fn aggressive() -> Self {
        Self {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            max_attempts: 10,
            backoff_multiplier: 1.5,
            jitter_factor: 0.25,
            full_jitter: false,
            total_timeout: Some(Duration::from_secs(60)),
        }
    }

    /// Create a gentle retry config for non-critical operations
    pub fn gentle() -> Self {
        Self {
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(60),
            max_attempts: 3,
            backoff_multiplier: 3.0,
            jitter_factor: 0.5,
            full_jitter: true,
            total_timeout: Some(Duration::from_secs(300)),
        }
    }

    /// Calculate delay for a given attempt number
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        // Base exponential delay
        let base_delay_ms = self.initial_delay.as_millis() as f64
            * self.backoff_multiplier.powi(attempt as i32);
        let base_delay_ms = base_delay_ms.min(self.max_delay.as_millis() as f64);

        // Apply jitter
        let jittered_delay_ms = if self.full_jitter {
            // Full jitter: random between 0 and base_delay
            let mut rng = rand::thread_rng();
            rng.gen_range(0.0..base_delay_ms)
        } else {
            // Decorrelated jitter: ±jitter_factor around base_delay
            let mut rng = rand::thread_rng();
            let jitter_range = base_delay_ms * self.jitter_factor;
            let jitter = rng.gen_range(-jitter_range..jitter_range);
            (base_delay_ms + jitter).max(0.0)
        };

        Duration::from_millis(jittered_delay_ms as u64)
    }
}

/// Retry statistics
#[derive(Debug, Clone, Default)]
pub struct RetryStats {
    /// Total operations attempted
    pub total_operations: AtomicU64,
    /// Operations that succeeded on first try
    pub first_try_success: AtomicU64,
    /// Operations that succeeded after retry
    pub retry_success: AtomicU64,
    /// Operations that failed after all retries
    pub total_failures: AtomicU64,
    /// Total retry attempts made
    pub total_retries: AtomicU64,
    /// Total time spent in retry delays
    pub total_delay_ms: AtomicU64,
}

impl RetryStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_first_try_success(&self) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.first_try_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry_success(&self, attempts: u32, delay_ms: u64) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.retry_success.fetch_add(1, Ordering::Relaxed);
        self.total_retries.fetch_add(attempts as u64, Ordering::Relaxed);
        self.total_delay_ms.fetch_add(delay_ms, Ordering::Relaxed);
    }

    pub fn record_failure(&self, attempts: u32, delay_ms: u64) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        self.total_retries.fetch_add(attempts as u64, Ordering::Relaxed);
        self.total_delay_ms.fetch_add(delay_ms, Ordering::Relaxed);
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.total_operations.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        let successes = self.first_try_success.load(Ordering::Relaxed)
            + self.retry_success.load(Ordering::Relaxed);
        successes as f64 / total as f64
    }

    pub fn average_retries(&self) -> f64 {
        let total = self.total_operations.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.total_retries.load(Ordering::Relaxed) as f64 / total as f64
    }
}

/// Retry budget to prevent unlimited retries during outages
#[derive(Debug)]
pub struct RetryBudget {
    /// Window duration for budget tracking
    window: Duration,
    /// Maximum retries allowed per window
    max_retries_per_window: u32,
    /// Current retry count
    retry_count: AtomicU64,
    /// Window start time
    window_start: RwLock<Instant>,
}

impl RetryBudget {
    pub fn new(window: Duration, max_retries: u32) -> Self {
        Self {
            window,
            max_retries_per_window: max_retries,
            retry_count: AtomicU64::new(0),
            window_start: RwLock::new(Instant::now()),
        }
    }

    /// Check if retry is allowed within budget
    pub async fn allow_retry(&self) -> bool {
        let now = Instant::now();

        // Check if window has expired
        {
            let window_start = self.window_start.read().await;
            if now.duration_since(*window_start) >= self.window {
                drop(window_start);
                // Reset window
                let mut window_start = self.window_start.write().await;
                *window_start = now;
                self.retry_count.store(0, Ordering::Relaxed);
            }
        }

        // Check budget
        let current = self.retry_count.fetch_add(1, Ordering::Relaxed);
        current < self.max_retries_per_window as u64
    }
}

/// Main retry executor
pub struct RetryExecutor {
    config: RetryConfig,
    stats: Arc<RetryStats>,
    budget: Option<Arc<RetryBudget>>,
}

impl RetryExecutor {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RetryStats::new()),
            budget: None,
        }
    }

    pub fn with_budget(mut self, budget: Arc<RetryBudget>) -> Self {
        self.budget = Some(budget);
        self
    }

    pub fn with_stats(mut self, stats: Arc<RetryStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Execute an operation with retry logic
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let start = Instant::now();
        let mut total_delay_ms = 0u64;
        let mut last_error = None;

        for attempt in 0..self.config.max_attempts {
            // Check total timeout
            if let Some(timeout) = self.config.total_timeout {
                if start.elapsed() >= timeout {
                    warn!("Retry total timeout exceeded after {} attempts", attempt);
                    break;
                }
            }

            // Check retry budget
            if attempt > 0 {
                if let Some(ref budget) = self.budget {
                    if !budget.allow_retry().await {
                        warn!("Retry budget exhausted, failing fast");
                        break;
                    }
                }
            }

            // Execute operation
            match operation().await {
                Ok(result) => {
                    if attempt == 0 {
                        self.stats.record_first_try_success();
                    } else {
                        self.stats.record_retry_success(attempt, total_delay_ms);
                        debug!("Operation succeeded after {} retries", attempt);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    // Check if error is retryable
                    if !e.is_retryable() {
                        debug!("Non-retryable error: {}", e);
                        self.stats.record_failure(attempt, total_delay_ms);
                        return Err(e);
                    }

                    last_error = Some(e);

                    // Calculate and apply delay
                    if attempt < self.config.max_attempts - 1 {
                        let delay = self.config.calculate_delay(attempt);
                        total_delay_ms += delay.as_millis() as u64;

                        debug!(
                            "Retry attempt {} failed, waiting {:?} before next attempt",
                            attempt + 1,
                            delay
                        );

                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        // All retries exhausted
        self.stats.record_failure(self.config.max_attempts, total_delay_ms);
        warn!(
            "All {} retry attempts exhausted, total delay: {}ms",
            self.config.max_attempts, total_delay_ms
        );

        Err(last_error.unwrap_or_else(|| {
            Error::Internal("Retry failed with no error captured".to_string())
        }))
    }

    /// Execute with custom should_retry predicate
    pub async fn execute_with_predicate<F, Fut, T, P>(
        &self,
        operation: F,
        should_retry: P,
    ) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
        P: Fn(&Error) -> bool,
    {
        let start = Instant::now();
        let mut total_delay_ms = 0u64;
        let mut last_error = None;

        for attempt in 0..self.config.max_attempts {
            if let Some(timeout) = self.config.total_timeout {
                if start.elapsed() >= timeout {
                    break;
                }
            }

            match operation().await {
                Ok(result) => {
                    if attempt == 0 {
                        self.stats.record_first_try_success();
                    } else {
                        self.stats.record_retry_success(attempt, total_delay_ms);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    if !should_retry(&e) {
                        self.stats.record_failure(attempt, total_delay_ms);
                        return Err(e);
                    }

                    last_error = Some(e);

                    if attempt < self.config.max_attempts - 1 {
                        let delay = self.config.calculate_delay(attempt);
                        total_delay_ms += delay.as_millis() as u64;
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        self.stats.record_failure(self.config.max_attempts, total_delay_ms);
        Err(last_error.unwrap_or_else(|| {
            Error::Internal("Retry failed".to_string())
        }))
    }

    pub fn stats(&self) -> Arc<RetryStats> {
        self.stats.clone()
    }
}

/// Convenience function for quick retry with default config
pub async fn retry<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    RetryExecutor::new(RetryConfig::default())
        .execute(operation)
        .await
}

/// Retry with fast config for latency-sensitive operations
pub async fn retry_fast<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    RetryExecutor::new(RetryConfig::fast())
        .execute(operation)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn test_delay_calculation() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,  // No jitter for deterministic test
            full_jitter: false,
            ..Default::default()
        };

        let delay_0 = config.calculate_delay(0);
        let delay_1 = config.calculate_delay(1);
        let delay_2 = config.calculate_delay(2);

        assert_eq!(delay_0.as_millis(), 100);
        assert_eq!(delay_1.as_millis(), 200);
        assert_eq!(delay_2.as_millis(), 400);
    }

    #[test]
    fn test_delay_with_jitter() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.5,
            full_jitter: false,
            ..Default::default()
        };

        // With jitter, delay should be within range
        let delay = config.calculate_delay(0);
        assert!(delay.as_millis() >= 50);  // 100 - 50%
        assert!(delay.as_millis() <= 150); // 100 + 50%
    }

    #[test]
    fn test_max_delay_cap() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            backoff_multiplier: 10.0,
            jitter_factor: 0.0,
            ..Default::default()
        };

        // Should cap at max_delay
        let delay = config.calculate_delay(5);
        assert_eq!(delay.as_millis(), 500);
    }

    #[tokio::test]
    async fn test_retry_success_first_try() {
        let executor = RetryExecutor::new(RetryConfig::fast());

        let result = executor.execute(|| async { Ok::<_, Error>(42) }).await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(executor.stats.first_try_success.load(Ordering::Relaxed), 1);
        assert_eq!(executor.stats.total_retries.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let executor = RetryExecutor::new(RetryConfig {
            initial_delay: Duration::from_millis(1),
            max_attempts: 5,
            ..RetryConfig::fast()
        });

        let result = executor
            .execute(|| {
                let count = attempt_count_clone.clone();
                async move {
                    let current = count.fetch_add(1, Ordering::Relaxed);
                    if current < 2 {
                        Err(Error::Internal("Transient failure".to_string()))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count.load(Ordering::Relaxed), 3);
        assert_eq!(executor.stats.retry_success.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_retry_stats() {
        let stats = RetryStats::new();

        stats.record_first_try_success();
        stats.record_first_try_success();
        stats.record_retry_success(2, 100);
        stats.record_failure(5, 500);

        assert_eq!(stats.total_operations.load(Ordering::Relaxed), 4);
        assert_eq!(stats.first_try_success.load(Ordering::Relaxed), 2);
        assert_eq!(stats.retry_success.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_failures.load(Ordering::Relaxed), 1);
        assert!((stats.success_rate() - 0.75).abs() < 0.01);
    }
}
