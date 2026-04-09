//! Position reconciliation module.
//!
//! Ensures consistency between:
//! - Local position state
//! - Exchange-reported positions
//! - Executed fills
//!
//! Handles:
//! - Position drift detection
//! - Orphaned position recovery
//! - Cross-venue position netting
//! - P&L reconciliation

pub mod position_tracker;
pub mod reconciler;
pub mod drift_detector;

pub use position_tracker::{PositionTracker, Position, PositionSide};
pub use reconciler::{PositionReconciler, ReconciliationResult, Discrepancy};
pub use drift_detector::{DriftDetector, DriftAlert, DriftSeverity};
