//! Execution engine for order management.
//!
//! Features:
//! - Smart order routing
//! - Atomic execution for arbitrage
//! - Order lifecycle management
//! - Execution analytics
//! - Cross-platform execution with leg sequencing
//! - Multi-venue state coordination and recovery

pub mod engine;
pub mod router;
pub mod atomic;
pub mod cross_platform;
pub mod state_coordinator;

pub use engine::ExecutionEngine;
pub use router::SmartOrderRouter;
pub use atomic::AtomicExecutor;
pub use cross_platform::{
    CrossPlatformExecutor, CrossPlatformConfig, CrossPlatformOpportunity,
    CrossPlatformLeg, ExecutionRecord, ExecutionState, FilledLegInfo,
    Venue, AbortReason,
};
pub use state_coordinator::{
    StateCoordinator, StateCoordinatorConfig, AggregatedPosition,
    VenuePosition, PendingRecovery, RecoveryState, ExposureSummary, VenueHealth,
};
