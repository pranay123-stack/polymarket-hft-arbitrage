//! Execution engine for order management.
//!
//! Features:
//! - Smart order routing
//! - Atomic execution for arbitrage
//! - Order lifecycle management
//! - Execution analytics
//! - Cross-platform execution with leg sequencing
//! - Multi-venue state coordination and recovery
//! - Order amendment/modification without race conditions
//! - Algorithmic execution (TWAP/VWAP/POV) for large orders
//! - Persistent state storage and crash recovery

pub mod engine;
pub mod router;
pub mod atomic;
pub mod cross_platform;
pub mod state_coordinator;
pub mod order_amendment;
pub mod algo_execution;
pub mod persistence;

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
pub use order_amendment::{
    OrderAmendmentEngine, AmendmentRequest, AmendmentResult, AmendmentStrategy,
    AmendmentStats,
};
pub use algo_execution::{
    AlgoExecutionEngine, AlgoConfig, AlgoType, AlgoState, AlgoSlice, AlgoResult,
    SliceStatus, ExecutionQuality, VolumeProfile,
};
pub use persistence::{
    ExecutionPersistence, PersistenceConfig, RecoveryTask, ExecutionStats,
};
