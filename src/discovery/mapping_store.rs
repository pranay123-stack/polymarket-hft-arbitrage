//! Persistent storage for market mappings.
//!
//! Stores verified market mappings in PostgreSQL for:
//! - Fast lookup during trading
//! - Audit trail of mapping decisions
//! - Manual verification workflows

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls, Error as PgError};
use uuid::Uuid;

use super::semantic_matcher::{MatchCandidate, MatchConfidence, MatchScore};

/// A stored market mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMapping {
    /// Unique mapping ID.
    pub id: Uuid,
    /// Source venue.
    pub source_venue: String,
    /// Source market ID.
    pub source_market_id: String,
    /// Target venue.
    pub target_venue: String,
    /// Target market ID.
    pub target_market_id: String,
    /// Match confidence.
    pub confidence: MatchConfidence,
    /// Match score.
    pub score: f64,
    /// Status of the mapping.
    pub status: MappingStatus,
    /// Who verified (if manually verified).
    pub verified_by: Option<String>,
    /// When the mapping was created.
    pub created_at: i64,
    /// When the mapping was last updated.
    pub updated_at: i64,
    /// When the mapping was verified.
    pub verified_at: Option<i64>,
    /// Additional metadata.
    pub metadata: HashMap<String, String>,
}

/// Status of a mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MappingStatus {
    /// Pending verification.
    Pending,
    /// Auto-verified (high confidence).
    AutoVerified,
    /// Manually verified.
    ManuallyVerified,
    /// Rejected as incorrect.
    Rejected,
    /// Expired (market resolved).
    Expired,
}

impl MappingStatus {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "pending" => MappingStatus::Pending,
            "auto_verified" | "autoverified" => MappingStatus::AutoVerified,
            "manually_verified" | "manuallyverified" => MappingStatus::ManuallyVerified,
            "rejected" => MappingStatus::Rejected,
            "expired" => MappingStatus::Expired,
            _ => MappingStatus::Pending,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MappingStatus::Pending => "pending",
            MappingStatus::AutoVerified => "auto_verified",
            MappingStatus::ManuallyVerified => "manually_verified",
            MappingStatus::Rejected => "rejected",
            MappingStatus::Expired => "expired",
        }
    }
}

/// Persistent store for market mappings.
pub struct MappingStore {
    /// Database connection string.
    connection_string: String,
    /// Database client (when connected).
    client: Option<Client>,
    /// In-memory cache.
    cache: HashMap<String, StoredMapping>,
    /// Whether auto-verification is enabled.
    auto_verify_high_confidence: bool,
    /// Minimum score for auto-verification.
    auto_verify_threshold: f64,
}

impl MappingStore {
    /// Create a new mapping store.
    pub fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            client: None,
            cache: HashMap::new(),
            auto_verify_high_confidence: true,
            auto_verify_threshold: 0.90,
        }
    }

    /// Connect to the database.
    pub async fn connect(&mut self) -> Result<(), PgError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Database connection error: {}", e);
            }
        });

        self.client = Some(client);
        self.initialize_schema().await?;
        self.load_cache().await?;

        Ok(())
    }

    /// Initialize the database schema.
    async fn initialize_schema(&self) -> Result<(), PgError> {
        let client = self.client.as_ref().ok_or_else(|| {
            PgError::__private_api_timeout()
        })?;

        client.execute(
            r#"
            CREATE TABLE IF NOT EXISTS market_mappings (
                id UUID PRIMARY KEY,
                source_venue VARCHAR(64) NOT NULL,
                source_market_id VARCHAR(256) NOT NULL,
                target_venue VARCHAR(64) NOT NULL,
                target_market_id VARCHAR(256) NOT NULL,
                confidence VARCHAR(32) NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                status VARCHAR(32) NOT NULL,
                verified_by VARCHAR(128),
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                verified_at BIGINT,
                metadata JSONB DEFAULT '{}',
                UNIQUE(source_venue, source_market_id, target_venue, target_market_id)
            )
            "#,
            &[],
        ).await?;

        // Create indexes
        client.execute(
            "CREATE INDEX IF NOT EXISTS idx_mappings_source ON market_mappings(source_venue, source_market_id)",
            &[],
        ).await?;

        client.execute(
            "CREATE INDEX IF NOT EXISTS idx_mappings_target ON market_mappings(target_venue, target_market_id)",
            &[],
        ).await?;

        client.execute(
            "CREATE INDEX IF NOT EXISTS idx_mappings_status ON market_mappings(status)",
            &[],
        ).await?;

        Ok(())
    }

    /// Load all active mappings into cache.
    async fn load_cache(&mut self) -> Result<(), PgError> {
        let client = self.client.as_ref().ok_or_else(|| {
            PgError::__private_api_timeout()
        })?;

        let rows = client.query(
            r#"
            SELECT id, source_venue, source_market_id, target_venue, target_market_id,
                   confidence, score, status, verified_by, created_at, updated_at, verified_at, metadata
            FROM market_mappings
            WHERE status IN ('auto_verified', 'manually_verified')
            "#,
            &[],
        ).await?;

        for row in rows {
            let id: Uuid = row.get(0);
            let source_venue: String = row.get(1);
            let source_market_id: String = row.get(2);
            let target_venue: String = row.get(3);
            let target_market_id: String = row.get(4);
            let confidence_str: String = row.get(5);
            let score: f64 = row.get(6);
            let status_str: String = row.get(7);
            let verified_by: Option<String> = row.get(8);
            let created_at: i64 = row.get(9);
            let updated_at: i64 = row.get(10);
            let verified_at: Option<i64> = row.get(11);
            let metadata_json: serde_json::Value = row.get(12);

            let confidence = match confidence_str.as_str() {
                "high" => MatchConfidence::High,
                "medium" => MatchConfidence::Medium,
                "low" => MatchConfidence::Low,
                _ => MatchConfidence::VeryLow,
            };

            let metadata: HashMap<String, String> = serde_json::from_value(metadata_json)
                .unwrap_or_default();

            let mapping = StoredMapping {
                id,
                source_venue: source_venue.clone(),
                source_market_id: source_market_id.clone(),
                target_venue,
                target_market_id,
                confidence,
                score,
                status: MappingStatus::from_str(&status_str),
                verified_by,
                created_at,
                updated_at,
                verified_at,
                metadata,
            };

            let key = format!("{}:{}", source_venue, source_market_id);
            self.cache.insert(key, mapping);
        }

        Ok(())
    }

    /// Store a new mapping from a match candidate.
    pub async fn store_mapping(&mut self, candidate: &MatchCandidate) -> Result<StoredMapping, PgError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let id = Uuid::new_v4();

        let status = if self.auto_verify_high_confidence && candidate.score >= self.auto_verify_threshold {
            MappingStatus::AutoVerified
        } else {
            MappingStatus::Pending
        };

        let confidence_str = match candidate.confidence {
            MatchConfidence::High => "high",
            MatchConfidence::Medium => "medium",
            MatchConfidence::Low => "low",
            MatchConfidence::VeryLow => "very_low",
        };

        let metadata: HashMap<String, String> = HashMap::new();
        let metadata_json = serde_json::to_value(&metadata).unwrap_or(serde_json::Value::Null);

        if let Some(client) = &self.client {
            client.execute(
                r#"
                INSERT INTO market_mappings
                    (id, source_venue, source_market_id, target_venue, target_market_id,
                     confidence, score, status, created_at, updated_at, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (source_venue, source_market_id, target_venue, target_market_id)
                DO UPDATE SET
                    score = $7,
                    status = $8,
                    updated_at = $10
                "#,
                &[
                    &id,
                    &candidate.source_venue,
                    &candidate.source_id,
                    &candidate.target_venue,
                    &candidate.target_id,
                    &confidence_str,
                    &candidate.score,
                    &status.as_str(),
                    &now,
                    &now,
                    &metadata_json,
                ],
            ).await?;
        }

        let mapping = StoredMapping {
            id,
            source_venue: candidate.source_venue.clone(),
            source_market_id: candidate.source_id.clone(),
            target_venue: candidate.target_venue.clone(),
            target_market_id: candidate.target_id.clone(),
            confidence: candidate.confidence,
            score: candidate.score,
            status,
            verified_by: None,
            created_at: now,
            updated_at: now,
            verified_at: if status == MappingStatus::AutoVerified { Some(now) } else { None },
            metadata,
        };

        // Update cache
        let key = format!("{}:{}", candidate.source_venue, candidate.source_id);
        self.cache.insert(key, mapping.clone());

        Ok(mapping)
    }

    /// Get mapping for a source market.
    pub fn get_mapping(&self, source_venue: &str, source_market_id: &str) -> Option<&StoredMapping> {
        let key = format!("{}:{}", source_venue, source_market_id);
        self.cache.get(&key)
    }

    /// Get all mappings for a venue.
    pub fn get_mappings_for_venue(&self, venue: &str) -> Vec<&StoredMapping> {
        self.cache
            .values()
            .filter(|m| m.source_venue == venue || m.target_venue == venue)
            .collect()
    }

    /// Get all verified mappings.
    pub fn get_verified_mappings(&self) -> Vec<&StoredMapping> {
        self.cache
            .values()
            .filter(|m| {
                m.status == MappingStatus::AutoVerified || m.status == MappingStatus::ManuallyVerified
            })
            .collect()
    }

    /// Get pending mappings for review.
    pub async fn get_pending_mappings(&self) -> Result<Vec<StoredMapping>, PgError> {
        let client = self.client.as_ref().ok_or_else(|| {
            PgError::__private_api_timeout()
        })?;

        let rows = client.query(
            r#"
            SELECT id, source_venue, source_market_id, target_venue, target_market_id,
                   confidence, score, status, verified_by, created_at, updated_at, verified_at, metadata
            FROM market_mappings
            WHERE status = 'pending'
            ORDER BY score DESC
            "#,
            &[],
        ).await?;

        let mut mappings = Vec::new();
        for row in rows {
            let id: Uuid = row.get(0);
            let source_venue: String = row.get(1);
            let source_market_id: String = row.get(2);
            let target_venue: String = row.get(3);
            let target_market_id: String = row.get(4);
            let confidence_str: String = row.get(5);
            let score: f64 = row.get(6);
            let status_str: String = row.get(7);
            let verified_by: Option<String> = row.get(8);
            let created_at: i64 = row.get(9);
            let updated_at: i64 = row.get(10);
            let verified_at: Option<i64> = row.get(11);
            let metadata_json: serde_json::Value = row.get(12);

            let confidence = match confidence_str.as_str() {
                "high" => MatchConfidence::High,
                "medium" => MatchConfidence::Medium,
                "low" => MatchConfidence::Low,
                _ => MatchConfidence::VeryLow,
            };

            let metadata: HashMap<String, String> = serde_json::from_value(metadata_json)
                .unwrap_or_default();

            mappings.push(StoredMapping {
                id,
                source_venue,
                source_market_id,
                target_venue,
                target_market_id,
                confidence,
                score,
                status: MappingStatus::from_str(&status_str),
                verified_by,
                created_at,
                updated_at,
                verified_at,
                metadata,
            });
        }

        Ok(mappings)
    }

    /// Verify a mapping manually.
    pub async fn verify_mapping(
        &mut self,
        mapping_id: Uuid,
        verified_by: &str,
        approve: bool,
    ) -> Result<(), PgError> {
        let client = self.client.as_ref().ok_or_else(|| {
            PgError::__private_api_timeout()
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let new_status = if approve {
            MappingStatus::ManuallyVerified
        } else {
            MappingStatus::Rejected
        };

        client.execute(
            r#"
            UPDATE market_mappings
            SET status = $1, verified_by = $2, verified_at = $3, updated_at = $4
            WHERE id = $5
            "#,
            &[
                &new_status.as_str(),
                &verified_by,
                &now,
                &now,
                &mapping_id,
            ],
        ).await?;

        // Update cache
        for mapping in self.cache.values_mut() {
            if mapping.id == mapping_id {
                mapping.status = new_status;
                mapping.verified_by = Some(verified_by.to_string());
                mapping.verified_at = Some(now);
                mapping.updated_at = now;
                break;
            }
        }

        Ok(())
    }

    /// Expire mappings for resolved markets.
    pub async fn expire_mapping(&mut self, source_venue: &str, source_market_id: &str) -> Result<(), PgError> {
        let client = self.client.as_ref().ok_or_else(|| {
            PgError::__private_api_timeout()
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        client.execute(
            r#"
            UPDATE market_mappings
            SET status = 'expired', updated_at = $1
            WHERE source_venue = $2 AND source_market_id = $3
            "#,
            &[&now, &source_venue, &source_market_id],
        ).await?;

        // Remove from cache
        let key = format!("{}:{}", source_venue, source_market_id);
        self.cache.remove(&key);

        Ok(())
    }

    /// Get mapping statistics.
    pub fn get_stats(&self) -> MappingStats {
        let total = self.cache.len();
        let auto_verified = self.cache.values().filter(|m| m.status == MappingStatus::AutoVerified).count();
        let manually_verified = self.cache.values().filter(|m| m.status == MappingStatus::ManuallyVerified).count();
        let pending = self.cache.values().filter(|m| m.status == MappingStatus::Pending).count();
        let rejected = self.cache.values().filter(|m| m.status == MappingStatus::Rejected).count();

        let avg_score = if total > 0 {
            self.cache.values().map(|m| m.score).sum::<f64>() / total as f64
        } else {
            0.0
        };

        MappingStats {
            total_mappings: total,
            auto_verified,
            manually_verified,
            pending,
            rejected,
            avg_score,
        }
    }
}

/// Statistics about stored mappings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingStats {
    pub total_mappings: usize,
    pub auto_verified: usize,
    pub manually_verified: usize,
    pub pending: usize,
    pub rejected: usize,
    pub avg_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping_status() {
        assert_eq!(MappingStatus::from_str("pending"), MappingStatus::Pending);
        assert_eq!(MappingStatus::from_str("AUTO_VERIFIED"), MappingStatus::AutoVerified);
        assert_eq!(MappingStatus::AutoVerified.as_str(), "auto_verified");
    }

    #[test]
    fn test_mapping_store_creation() {
        let store = MappingStore::new("postgres://localhost/test");
        assert!(store.cache.is_empty());
    }
}
