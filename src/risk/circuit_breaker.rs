//! Circuit breaker for automatic trading halt.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::time::Duration;
use tracing::{info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for automatic trading halt
pub struct CircuitBreaker {
    state: RwLock<CircuitBreakerState>,
    cooldown_duration: Duration,
    tripped_at: RwLock<Option<DateTime<Utc>>>,
    trip_reason: RwLock<Option<String>>,
    trip_count: std::sync::atomic::AtomicU32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(cooldown_duration: Duration) -> Self {
        Self {
            state: RwLock::new(CircuitBreakerState::Closed),
            cooldown_duration,
            tripped_at: RwLock::new(None),
            trip_reason: RwLock::new(None),
            trip_count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Check if circuit breaker is open (trading halted)
    pub fn is_open(&self) -> bool {
        let state = *self.state.read();

        match state {
            CircuitBreakerState::Open => {
                // Check if cooldown has elapsed
                if let Some(tripped) = *self.tripped_at.read() {
                    let elapsed = Utc::now() - tripped;
                    if elapsed > chrono::Duration::from_std(self.cooldown_duration).unwrap() {
                        // Move to half-open
                        *self.state.write() = CircuitBreakerState::HalfOpen;
                        info!("Circuit breaker moved to half-open state");
                        return false;
                    }
                }
                true
            }
            CircuitBreakerState::HalfOpen => false,
            CircuitBreakerState::Closed => false,
        }
    }

    /// Trip the circuit breaker
    pub fn trip(&self, reason: &str) {
        let mut state = self.state.write();
        *state = CircuitBreakerState::Open;

        *self.tripped_at.write() = Some(Utc::now());
        *self.trip_reason.write() = Some(reason.to_string());
        self.trip_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        warn!("Circuit breaker tripped: {}", reason);
    }

    /// Record a successful operation (in half-open state)
    pub fn record_success(&self) {
        let mut state = self.state.write();
        if *state == CircuitBreakerState::HalfOpen {
            *state = CircuitBreakerState::Closed;
            *self.tripped_at.write() = None;
            *self.trip_reason.write() = None;
            info!("Circuit breaker closed after successful operation");
        }
    }

    /// Record a failure (in half-open state)
    pub fn record_failure(&self, reason: &str) {
        let state = *self.state.read();
        if state == CircuitBreakerState::HalfOpen {
            self.trip(reason);
        }
    }

    /// Get remaining cooldown time
    pub fn cooldown_remaining(&self) -> Option<Duration> {
        if *self.state.read() != CircuitBreakerState::Open {
            return None;
        }

        if let Some(tripped) = *self.tripped_at.read() {
            let elapsed = Utc::now() - tripped;
            let elapsed_std = elapsed.to_std().unwrap_or(Duration::ZERO);

            if elapsed_std < self.cooldown_duration {
                return Some(self.cooldown_duration - elapsed_std);
            }
        }

        None
    }

    /// Get current state
    pub fn state(&self) -> CircuitBreakerState {
        // First check if we should transition
        let _ = self.is_open();
        *self.state.read()
    }

    /// Get trip reason
    pub fn trip_reason(&self) -> Option<String> {
        self.trip_reason.read().clone()
    }

    /// Get trip count
    pub fn trip_count(&self) -> u32 {
        self.trip_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reset the circuit breaker
    pub fn reset(&self) {
        *self.state.write() = CircuitBreakerState::Closed;
        *self.tripped_at.write() = None;
        *self.trip_reason.write() = None;
        info!("Circuit breaker reset");
    }

    /// Force close the circuit breaker (manual override)
    pub fn force_close(&self) {
        self.reset();
        warn!("Circuit breaker force closed");
    }
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("state", &*self.state.read())
            .field("cooldown_duration", &self.cooldown_duration)
            .field("trip_count", &self.trip_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new(Duration::from_secs(60));
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
    }

    #[test]
    fn test_circuit_breaker_trip() {
        let cb = CircuitBreaker::new(Duration::from_secs(60));

        cb.trip("Test reason");

        assert!(cb.is_open());
        assert_eq!(cb.state(), CircuitBreakerState::Open);
        assert_eq!(cb.trip_reason(), Some("Test reason".to_string()));
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(Duration::from_secs(60));

        cb.trip("Test reason");
        assert!(cb.is_open());

        cb.reset();
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
    }

    #[test]
    fn test_circuit_breaker_cooldown() {
        let cb = CircuitBreaker::new(Duration::from_millis(100));

        cb.trip("Test reason");
        assert!(cb.is_open());

        // Wait for cooldown
        std::thread::sleep(Duration::from_millis(150));

        // Should now be half-open
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitBreakerState::HalfOpen);
    }
}
