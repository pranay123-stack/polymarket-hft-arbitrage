//! Rate limiter for API requests.
//!
//! Implements a token bucket algorithm with configurable rates
//! to prevent API rate limit violations.

use governor::{Quota, RateLimiter as GovernorLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Rate limiter using token bucket algorithm
pub struct RateLimiter {
    limiter: Arc<GovernorLimiter<governor::state::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>>,
    requests_per_second: u32,
}

impl RateLimiter {
    /// Create a new rate limiter with the given requests per second
    pub fn new(requests_per_second: u32) -> Self {
        let rps = NonZeroU32::new(requests_per_second.max(1)).unwrap();
        let quota = Quota::per_second(rps);
        let limiter = Arc::new(GovernorLimiter::direct(quota));

        Self {
            limiter,
            requests_per_second,
        }
    }

    /// Acquire a permit, waiting if necessary
    pub async fn acquire(&self) {
        self.limiter.until_ready().await;
    }

    /// Try to acquire a permit without waiting
    pub fn try_acquire(&self) -> bool {
        self.limiter.check().is_ok()
    }

    /// Get the configured rate limit
    pub fn rate_limit(&self) -> u32 {
        self.requests_per_second
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            requests_per_second: self.requests_per_second,
        }
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("requests_per_second", &self.requests_per_second)
            .finish()
    }
}

/// Adaptive rate limiter that adjusts based on response headers
pub struct AdaptiveRateLimiter {
    base_limiter: RateLimiter,
    backoff_multiplier: f64,
    current_delay_ms: std::sync::atomic::AtomicU64,
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter
    pub fn new(requests_per_second: u32) -> Self {
        Self {
            base_limiter: RateLimiter::new(requests_per_second),
            backoff_multiplier: 2.0,
            current_delay_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Acquire a permit, waiting if necessary
    pub async fn acquire(&self) {
        // Wait for base rate limit
        self.base_limiter.acquire().await;

        // Add any additional backoff delay
        let delay = self.current_delay_ms.load(std::sync::atomic::Ordering::Relaxed);
        if delay > 0 {
            sleep(Duration::from_millis(delay)).await;
        }
    }

    /// Signal that a rate limit was hit
    pub fn on_rate_limited(&self, retry_after_secs: u64) {
        let new_delay = retry_after_secs * 1000;
        self.current_delay_ms.store(new_delay, std::sync::atomic::Ordering::Relaxed);
    }

    /// Signal a successful request
    pub fn on_success(&self) {
        let current = self.current_delay_ms.load(std::sync::atomic::Ordering::Relaxed);
        if current > 0 {
            // Gradually reduce delay
            let new_delay = (current as f64 * 0.9) as u64;
            self.current_delay_ms.store(new_delay, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Reset the backoff
    pub fn reset(&self) {
        self.current_delay_ms.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clone for AdaptiveRateLimiter {
    fn clone(&self) -> Self {
        Self {
            base_limiter: self.base_limiter.clone(),
            backoff_multiplier: self.backoff_multiplier,
            current_delay_ms: std::sync::atomic::AtomicU64::new(
                self.current_delay_ms.load(std::sync::atomic::Ordering::Relaxed)
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(10);

        let start = Instant::now();

        // Acquire 10 permits (should be nearly instant)
        for _ in 0..10 {
            limiter.acquire().await;
        }

        let elapsed = start.elapsed();
        // Should complete within 2 seconds (rate is 10/sec)
        assert!(elapsed < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn test_try_acquire() {
        let limiter = RateLimiter::new(1);

        // First acquire should succeed
        assert!(limiter.try_acquire());

        // Second immediate acquire should fail (rate limited)
        // (This is probabilistic, so we give it a few tries)
        let mut all_succeeded = true;
        for _ in 0..5 {
            if !limiter.try_acquire() {
                all_succeeded = false;
                break;
            }
        }
        // At least one should have been rate limited
        assert!(!all_succeeded);
    }
}
