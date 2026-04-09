//! Market discovery and mapping service.
//!
//! Automatically discovers and maintains mappings between equivalent markets
//! across different prediction market venues (Polymarket, Kalshi, Opinion).
//!
//! Features:
//! - Automatic market discovery from venue APIs
//! - Semantic matching of equivalent markets
//! - Confidence scoring for mappings
//! - Persistent storage of verified mappings
//! - Real-time mapping updates

pub mod market_mapper;
pub mod semantic_matcher;
pub mod venue_scanner;
pub mod mapping_store;

pub use market_mapper::{MarketMapper, MarketMapping, MappingStatus};
pub use semantic_matcher::{SemanticMatcher, MatchScore, MatchCandidate};
pub use venue_scanner::{VenueScanner, DiscoveredMarket, MarketCategory};
pub use mapping_store::{MappingStore, StoredMapping};
