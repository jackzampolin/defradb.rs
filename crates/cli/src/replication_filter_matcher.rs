//! Query-filter-backed replication filter matcher.
//!
//! Evaluates [`p2p::ReplicationFilter`] predicates by delegating to the
//! DefraDB query filter engine (`query::Filter`), enabling the full
//! operator set (`_in`, `_eq`, `_gt`, `_and`, `_or`, `_not`, …).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;

use p2p::{ReplicationFilter, ReplicationFilterMatcher};
use query::Filter;

/// Evaluates replication filters using the DefraDB query filter engine.
///
/// Parsed [`Filter`] objects are cached by a hash of the conditions JSON so
/// re-parsing is avoided when the same predicate is applied to many documents.
pub struct QueryReplicationFilterMatcher {
    cache: Mutex<HashMap<u64, Filter>>,
}

impl QueryReplicationFilterMatcher {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn eval(&self, filter: &ReplicationFilter, document: &serde_json::Value) -> bool {
        match filter {
            ReplicationFilter::Predicate(conds) => {
                let key = hash_conditions(conds);
                let parsed = {
                    let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                    cache
                        .entry(key)
                        .or_insert_with(|| Filter::from_conditions(conds.clone()))
                        .clone()
                };
                parsed.matches_json_object(document).unwrap_or(false)
            }
            ReplicationFilter::All(subs) => subs.iter().all(|s| self.eval(s, document)),
            ReplicationFilter::Acp { .. } => false,
        }
    }
}

impl Default for QueryReplicationFilterMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationFilterMatcher for QueryReplicationFilterMatcher {
    fn matches(
        &self,
        _collection_id: &str,
        filter: &ReplicationFilter,
        document: &serde_json::Value,
    ) -> bool {
        self.eval(filter, document)
    }
}

fn hash_conditions(conds: &serde_json::Map<String, serde_json::Value>) -> u64 {
    let mut hasher = DefaultHasher::new();
    serde_json::to_string(conds)
        .unwrap_or_default()
        .hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn matcher() -> QueryReplicationFilterMatcher {
        QueryReplicationFilterMatcher::new()
    }

    fn predicate(conds: serde_json::Value) -> ReplicationFilter {
        ReplicationFilter::Predicate(conds.as_object().unwrap().clone())
    }

    #[test]
    fn predicate_in_matches_and_excludes() {
        let m = matcher();
        let f = predicate(json!({"agent_did": {"_in": ["did:a", "did:b"]}}));

        assert!(m.matches("col", &f, &json!({"agent_did": "did:a", "x": 1})));
        assert!(m.matches("col", &f, &json!({"agent_did": "did:b"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:c"})));
    }

    #[test]
    fn predicate_composite_and() {
        let m = matcher();
        let f = predicate(json!({"agent_did": {"_eq": "did:a"}, "kind": {"_eq": "x"}}));

        assert!(m.matches("col", &f, &json!({"agent_did": "did:a", "kind": "x"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:a", "kind": "y"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:b", "kind": "x"})));
    }

    #[test]
    fn predicate_typed_values() {
        let m = matcher();

        let score_f = predicate(json!({"score": {"_eq": 2}}));
        assert!(m.matches("col", &score_f, &json!({"score": 2})));
        assert!(!m.matches("col", &score_f, &json!({"score": 3})));

        let bool_f = predicate(json!({"active": {"_eq": true}}));
        assert!(m.matches("col", &bool_f, &json!({"active": true})));
        assert!(!m.matches("col", &bool_f, &json!({"active": false})));
    }

    #[test]
    fn all_requires_all() {
        let m = matcher();
        let f = ReplicationFilter::All(vec![
            predicate(json!({"agent_did": {"_eq": "did:a"}})),
            predicate(json!({"kind": {"_eq": "x"}})),
        ]);

        assert!(m.matches("col", &f, &json!({"agent_did": "did:a", "kind": "x"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:a", "kind": "y"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:b", "kind": "x"})));
    }

    #[test]
    fn acp_never_matches() {
        let m = matcher();
        let f = ReplicationFilter::Acp {
            relation: "owner".to_string(),
        };
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:a"})));
        assert!(!m.matches("col", &f, &json!({})));
    }

    #[test]
    fn caching_is_consistent() {
        let m = matcher();
        let f = predicate(json!({"agent_did": {"_in": ["did:a", "did:b"]}}));

        assert!(m.matches("col", &f, &json!({"agent_did": "did:a"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:c"})));
        assert!(m.matches("col", &f, &json!({"agent_did": "did:b"})));
        assert!(!m.matches("col", &f, &json!({"agent_did": "did:z"})));
    }
}
