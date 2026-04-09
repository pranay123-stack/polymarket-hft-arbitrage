//! Semantic matching for market equivalence detection.
//!
//! Uses NLP techniques to match markets across venues:
//! - Token similarity
//! - Entity extraction (dates, names, events)
//! - Outcome normalization
//! - Category matching

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// A candidate match between two markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchCandidate {
    /// Source market identifier.
    pub source_id: String,
    /// Target market identifier.
    pub target_id: String,
    /// Source venue.
    pub source_venue: String,
    /// Target venue.
    pub target_venue: String,
    /// Overall match score (0.0 to 1.0).
    pub score: f64,
    /// Breakdown of score components.
    pub score_breakdown: MatchScore,
    /// Confidence level.
    pub confidence: MatchConfidence,
    /// Reasons for the match.
    pub match_reasons: Vec<String>,
}

/// Breakdown of match score components.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchScore {
    /// Title similarity score.
    pub title_similarity: f64,
    /// Description similarity score.
    pub description_similarity: f64,
    /// Category match score.
    pub category_match: f64,
    /// Entity overlap score (dates, names, etc.).
    pub entity_overlap: f64,
    /// Outcome structure similarity.
    pub outcome_similarity: f64,
    /// Resolution date proximity score.
    pub date_proximity: f64,
    /// Final weighted score.
    pub weighted_score: f64,
}

/// Confidence level for a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchConfidence {
    /// High confidence - automated execution safe.
    High,
    /// Medium confidence - manual review recommended.
    Medium,
    /// Low confidence - manual verification required.
    Low,
    /// Very low - likely not a match.
    VeryLow,
}

impl MatchConfidence {
    pub fn from_score(score: f64) -> Self {
        if score >= 0.90 {
            MatchConfidence::High
        } else if score >= 0.75 {
            MatchConfidence::Medium
        } else if score >= 0.50 {
            MatchConfidence::Low
        } else {
            MatchConfidence::VeryLow
        }
    }
}

/// Extracted entities from market text.
#[derive(Debug, Clone, Default)]
pub struct ExtractedEntities {
    pub dates: Vec<String>,
    pub people: Vec<String>,
    pub organizations: Vec<String>,
    pub locations: Vec<String>,
    pub numbers: Vec<String>,
    pub percentages: Vec<String>,
}

/// Configuration for semantic matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatcherConfig {
    /// Weight for title similarity.
    pub title_weight: f64,
    /// Weight for description similarity.
    pub description_weight: f64,
    /// Weight for category match.
    pub category_weight: f64,
    /// Weight for entity overlap.
    pub entity_weight: f64,
    /// Weight for outcome similarity.
    pub outcome_weight: f64,
    /// Weight for date proximity.
    pub date_weight: f64,
    /// Minimum score to consider a match.
    pub min_match_score: f64,
    /// Minimum confidence for automated pairing.
    pub min_auto_confidence: MatchConfidence,
}

impl Default for MatcherConfig {
    fn default() -> Self {
        Self {
            title_weight: 0.30,
            description_weight: 0.15,
            category_weight: 0.10,
            entity_weight: 0.20,
            outcome_weight: 0.15,
            date_weight: 0.10,
            min_match_score: 0.60,
            min_auto_confidence: MatchConfidence::High,
        }
    }
}

/// A market representation for matching.
#[derive(Debug, Clone)]
pub struct MarketForMatching {
    pub id: String,
    pub venue: String,
    pub title: String,
    pub description: String,
    pub category: Option<String>,
    pub outcomes: Vec<String>,
    pub resolution_date: Option<String>,
    pub created_at: Option<String>,
}

/// Semantic matcher for finding equivalent markets across venues.
pub struct SemanticMatcher {
    config: MatcherConfig,
    stopwords: HashSet<String>,
    category_mappings: HashMap<String, Vec<String>>,
}

impl SemanticMatcher {
    /// Create a new semantic matcher.
    pub fn new(config: MatcherConfig) -> Self {
        let stopwords = Self::default_stopwords();
        let category_mappings = Self::default_category_mappings();

        Self {
            config,
            stopwords,
            category_mappings,
        }
    }

    /// Find matches for a source market among target markets.
    pub fn find_matches(
        &self,
        source: &MarketForMatching,
        targets: &[MarketForMatching],
    ) -> Vec<MatchCandidate> {
        let mut candidates: Vec<MatchCandidate> = targets
            .iter()
            .filter(|t| t.venue != source.venue) // Don't match same venue
            .map(|target| self.calculate_match(source, target))
            .filter(|c| c.score >= self.config.min_match_score)
            .collect();

        // Sort by score descending
        candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        candidates
    }

    /// Calculate match score between two markets.
    fn calculate_match(&self, source: &MarketForMatching, target: &MarketForMatching) -> MatchCandidate {
        let mut score = MatchScore::default();
        let mut reasons = Vec::new();

        // Title similarity
        score.title_similarity = self.text_similarity(&source.title, &target.title);
        if score.title_similarity > 0.8 {
            reasons.push("Highly similar titles".to_string());
        }

        // Description similarity
        score.description_similarity = self.text_similarity(&source.description, &target.description);

        // Category match
        score.category_match = self.category_similarity(&source.category, &target.category);
        if score.category_match > 0.9 {
            reasons.push("Same category".to_string());
        }

        // Entity overlap
        let source_entities = self.extract_entities(&source.title, &source.description);
        let target_entities = self.extract_entities(&target.title, &target.description);
        score.entity_overlap = self.entity_similarity(&source_entities, &target_entities);
        if score.entity_overlap > 0.7 {
            reasons.push("Matching entities (dates, names)".to_string());
        }

        // Outcome similarity
        score.outcome_similarity = self.outcome_similarity(&source.outcomes, &target.outcomes);
        if score.outcome_similarity > 0.8 {
            reasons.push("Similar outcome structure".to_string());
        }

        // Date proximity
        score.date_proximity = self.date_similarity(&source.resolution_date, &target.resolution_date);
        if score.date_proximity > 0.9 {
            reasons.push("Same resolution date".to_string());
        }

        // Calculate weighted score
        score.weighted_score =
            score.title_similarity * self.config.title_weight +
            score.description_similarity * self.config.description_weight +
            score.category_match * self.config.category_weight +
            score.entity_overlap * self.config.entity_weight +
            score.outcome_similarity * self.config.outcome_weight +
            score.date_proximity * self.config.date_weight;

        let confidence = MatchConfidence::from_score(score.weighted_score);

        MatchCandidate {
            source_id: source.id.clone(),
            target_id: target.id.clone(),
            source_venue: source.venue.clone(),
            target_venue: target.venue.clone(),
            score: score.weighted_score,
            score_breakdown: score,
            confidence,
            match_reasons: reasons,
        }
    }

    /// Calculate text similarity using Jaccard index with tokenization.
    fn text_similarity(&self, text1: &str, text2: &str) -> f64 {
        let tokens1 = self.tokenize(text1);
        let tokens2 = self.tokenize(text2);

        if tokens1.is_empty() && tokens2.is_empty() {
            return 1.0;
        }
        if tokens1.is_empty() || tokens2.is_empty() {
            return 0.0;
        }

        let set1: HashSet<_> = tokens1.iter().collect();
        let set2: HashSet<_> = tokens2.iter().collect();

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union == 0 {
            0.0
        } else {
            intersection as f64 / union as f64
        }
    }

    /// Tokenize text into normalized tokens.
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty() && s.len() > 1)
            .filter(|s| !self.stopwords.contains(*s))
            .map(|s| self.normalize_token(s))
            .collect()
    }

    /// Normalize a token (stemming, standardization).
    fn normalize_token(&self, token: &str) -> String {
        // Simple normalization rules
        let token = token.to_lowercase();

        // Common substitutions
        match token.as_str() {
            "january" | "jan" => "01".to_string(),
            "february" | "feb" => "02".to_string(),
            "march" | "mar" => "03".to_string(),
            "april" | "apr" => "04".to_string(),
            "may" => "05".to_string(),
            "june" | "jun" => "06".to_string(),
            "july" | "jul" => "07".to_string(),
            "august" | "aug" => "08".to_string(),
            "september" | "sept" | "sep" => "09".to_string(),
            "october" | "oct" => "10".to_string(),
            "november" | "nov" => "11".to_string(),
            "december" | "dec" => "12".to_string(),
            "president" | "presidential" => "president".to_string(),
            "election" | "elections" => "election".to_string(),
            "bitcoin" | "btc" => "bitcoin".to_string(),
            "ethereum" | "eth" => "ethereum".to_string(),
            _ => token,
        }
    }

    /// Calculate category similarity.
    fn category_similarity(&self, cat1: &Option<String>, cat2: &Option<String>) -> f64 {
        match (cat1, cat2) {
            (Some(c1), Some(c2)) => {
                let c1_lower = c1.to_lowercase();
                let c2_lower = c2.to_lowercase();

                if c1_lower == c2_lower {
                    return 1.0;
                }

                // Check category mappings
                if let Some(synonyms) = self.category_mappings.get(&c1_lower) {
                    if synonyms.contains(&c2_lower) {
                        return 0.9;
                    }
                }

                // Partial match
                self.text_similarity(c1, c2)
            }
            (None, None) => 0.5, // Both unknown
            _ => 0.0,
        }
    }

    /// Extract entities from text.
    fn extract_entities(&self, title: &str, description: &str) -> ExtractedEntities {
        let combined = format!("{} {}", title, description);
        let mut entities = ExtractedEntities::default();

        // Extract dates (various formats)
        let date_patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"\d{1,2}/\d{1,2}/\d{2,4}",
            r"(?i)(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}",
            r"(?i)(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\.?\s+\d{1,2},?\s+\d{4}",
        ];

        for pattern in &date_patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                for cap in re.find_iter(&combined) {
                    entities.dates.push(cap.as_str().to_lowercase());
                }
            }
        }

        // Extract percentages
        if let Ok(re) = regex::Regex::new(r"\d+(?:\.\d+)?%") {
            for cap in re.find_iter(&combined) {
                entities.percentages.push(cap.as_str().to_string());
            }
        }

        // Extract numbers
        if let Ok(re) = regex::Regex::new(r"\b\d{4,}\b") {
            for cap in re.find_iter(&combined) {
                entities.numbers.push(cap.as_str().to_string());
            }
        }

        entities
    }

    /// Calculate entity similarity.
    fn entity_similarity(&self, ent1: &ExtractedEntities, ent2: &ExtractedEntities) -> f64 {
        let mut scores = Vec::new();

        // Date overlap
        if !ent1.dates.is_empty() || !ent2.dates.is_empty() {
            let set1: HashSet<_> = ent1.dates.iter().collect();
            let set2: HashSet<_> = ent2.dates.iter().collect();
            let intersection = set1.intersection(&set2).count();
            let union = set1.union(&set2).count();
            if union > 0 {
                scores.push(intersection as f64 / union as f64);
            }
        }

        // Number overlap
        if !ent1.numbers.is_empty() || !ent2.numbers.is_empty() {
            let set1: HashSet<_> = ent1.numbers.iter().collect();
            let set2: HashSet<_> = ent2.numbers.iter().collect();
            let intersection = set1.intersection(&set2).count();
            let union = set1.union(&set2).count();
            if union > 0 {
                scores.push(intersection as f64 / union as f64);
            }
        }

        // Percentage overlap
        if !ent1.percentages.is_empty() || !ent2.percentages.is_empty() {
            let set1: HashSet<_> = ent1.percentages.iter().collect();
            let set2: HashSet<_> = ent2.percentages.iter().collect();
            let intersection = set1.intersection(&set2).count();
            let union = set1.union(&set2).count();
            if union > 0 {
                scores.push(intersection as f64 / union as f64);
            }
        }

        if scores.is_empty() {
            0.5 // No entities to compare
        } else {
            scores.iter().sum::<f64>() / scores.len() as f64
        }
    }

    /// Calculate outcome similarity.
    fn outcome_similarity(&self, outcomes1: &[String], outcomes2: &[String]) -> f64 {
        if outcomes1.is_empty() && outcomes2.is_empty() {
            return 1.0;
        }
        if outcomes1.is_empty() || outcomes2.is_empty() {
            return 0.0;
        }

        // Normalize outcomes
        let norm1: Vec<String> = outcomes1.iter().map(|o| self.normalize_outcome(o)).collect();
        let norm2: Vec<String> = outcomes2.iter().map(|o| self.normalize_outcome(o)).collect();

        // Count matches
        let mut matched = 0;
        let mut matched_indices = HashSet::new();

        for o1 in &norm1 {
            for (idx, o2) in norm2.iter().enumerate() {
                if !matched_indices.contains(&idx) {
                    let sim = self.text_similarity(o1, o2);
                    if sim > 0.7 {
                        matched += 1;
                        matched_indices.insert(idx);
                        break;
                    }
                }
            }
        }

        let total = norm1.len().max(norm2.len());
        matched as f64 / total as f64
    }

    /// Normalize an outcome string.
    fn normalize_outcome(&self, outcome: &str) -> String {
        let lower = outcome.to_lowercase().trim().to_string();

        // Standard normalizations
        match lower.as_str() {
            "yes" | "true" | "will" | "passes" => "yes".to_string(),
            "no" | "false" | "won't" | "fails" => "no".to_string(),
            _ => lower,
        }
    }

    /// Calculate date similarity.
    fn date_similarity(&self, date1: &Option<String>, date2: &Option<String>) -> f64 {
        match (date1, date2) {
            (Some(d1), Some(d2)) => {
                if d1 == d2 {
                    return 1.0;
                }

                // Try to parse and compare dates
                if let (Ok(parsed1), Ok(parsed2)) = (
                    chrono::NaiveDate::parse_from_str(d1, "%Y-%m-%d"),
                    chrono::NaiveDate::parse_from_str(d2, "%Y-%m-%d"),
                ) {
                    let diff = (parsed1 - parsed2).num_days().abs();
                    if diff == 0 {
                        1.0
                    } else if diff <= 1 {
                        0.95
                    } else if diff <= 7 {
                        0.8
                    } else if diff <= 30 {
                        0.5
                    } else {
                        0.1
                    }
                } else {
                    // String comparison fallback
                    self.text_similarity(d1, d2)
                }
            }
            (None, None) => 0.5,
            _ => 0.0,
        }
    }

    /// Get default stopwords.
    fn default_stopwords() -> HashSet<String> {
        [
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "must", "shall", "can", "need", "dare",
            "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
            "from", "as", "into", "through", "during", "before", "after",
            "above", "below", "between", "under", "again", "further", "then",
            "once", "here", "there", "when", "where", "why", "how", "all",
            "each", "few", "more", "most", "other", "some", "such", "no",
            "nor", "not", "only", "own", "same", "so", "than", "too", "very",
            "just", "and", "but", "if", "or", "because", "until", "while",
            "this", "that", "these", "those", "what", "which", "who", "whom",
            "market", "bet", "prediction", "will", "price",
        ].iter().map(|s| s.to_string()).collect()
    }

    /// Get default category mappings.
    fn default_category_mappings() -> HashMap<String, Vec<String>> {
        let mut mappings = HashMap::new();

        mappings.insert("politics".to_string(), vec![
            "political".to_string(),
            "elections".to_string(),
            "government".to_string(),
        ]);

        mappings.insert("sports".to_string(), vec![
            "sporting".to_string(),
            "athletics".to_string(),
            "games".to_string(),
        ]);

        mappings.insert("crypto".to_string(), vec![
            "cryptocurrency".to_string(),
            "bitcoin".to_string(),
            "blockchain".to_string(),
            "defi".to_string(),
        ]);

        mappings.insert("finance".to_string(), vec![
            "financial".to_string(),
            "markets".to_string(),
            "economics".to_string(),
            "stocks".to_string(),
        ]);

        mappings.insert("entertainment".to_string(), vec![
            "movies".to_string(),
            "tv".to_string(),
            "celebrities".to_string(),
            "awards".to_string(),
        ]);

        mappings
    }
}

impl Default for SemanticMatcher {
    fn default() -> Self {
        Self::new(MatcherConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_similarity() {
        let matcher = SemanticMatcher::default();

        // Identical texts
        let sim = matcher.text_similarity(
            "Will Bitcoin reach $100,000 by 2024?",
            "Will Bitcoin reach $100,000 by 2024?",
        );
        assert!(sim > 0.99);

        // Similar texts
        let sim = matcher.text_similarity(
            "Will Bitcoin reach $100,000 by 2024?",
            "Bitcoin to hit $100,000 in 2024",
        );
        assert!(sim > 0.5);

        // Different texts
        let sim = matcher.text_similarity(
            "Will Bitcoin reach $100,000?",
            "Who will win the election?",
        );
        assert!(sim < 0.3);
    }

    #[test]
    fn test_outcome_similarity() {
        let matcher = SemanticMatcher::default();

        // Binary markets
        let sim = matcher.outcome_similarity(
            &["Yes".to_string(), "No".to_string()],
            &["Yes".to_string(), "No".to_string()],
        );
        assert!(sim > 0.99);

        // Yes/No vs True/False
        let sim = matcher.outcome_similarity(
            &["Yes".to_string(), "No".to_string()],
            &["True".to_string(), "False".to_string()],
        );
        assert!(sim > 0.8);
    }

    #[test]
    fn test_match_calculation() {
        let matcher = SemanticMatcher::default();

        let source = MarketForMatching {
            id: "poly_btc_100k".to_string(),
            venue: "polymarket".to_string(),
            title: "Will Bitcoin reach $100,000 by December 31, 2024?".to_string(),
            description: "This market resolves YES if Bitcoin price exceeds $100,000".to_string(),
            category: Some("Crypto".to_string()),
            outcomes: vec!["Yes".to_string(), "No".to_string()],
            resolution_date: Some("2024-12-31".to_string()),
            created_at: None,
        };

        let target = MarketForMatching {
            id: "kalshi_btc_100k".to_string(),
            venue: "kalshi".to_string(),
            title: "Bitcoin above $100,000 by end of 2024".to_string(),
            description: "Resolves to Yes if BTC exceeds 100000 USD".to_string(),
            category: Some("Cryptocurrency".to_string()),
            outcomes: vec!["Yes".to_string(), "No".to_string()],
            resolution_date: Some("2024-12-31".to_string()),
            created_at: None,
        };

        let candidates = matcher.find_matches(&source, &[target]);

        assert!(!candidates.is_empty());
        let best = &candidates[0];
        assert!(best.score > 0.7);
        assert_eq!(best.confidence, MatchConfidence::High);
    }
}
