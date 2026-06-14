use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;

use p2p::{ReplicationFilter, ReplicationFilterMatcher};
use query::Filter;
use schema::{CType, FieldDescription, FieldKind};
use serde_json::Value as JsonValue;

/// Validate a replication filter against a collection's schema fields.
///
/// Every field referenced anywhere in the predicate must be @immutable scalar-LWW,
/// and every operator/value must be supported and type-compatible.
pub fn validate_replication_filter(
    fields: &[FieldDescription],
    collection_id: &str,
    filter: &ReplicationFilter,
) -> Result<(), String> {
    match filter {
        ReplicationFilter::Acp { .. } => {
            Err("acp-scoped replication filters are not yet implemented (see #1036)".to_string())
        }
        ReplicationFilter::All(subs) => subs
            .iter()
            .try_for_each(|f| validate_replication_filter(fields, collection_id, f)),
        ReplicationFilter::Predicate(conds) => validate_conditions(conds, fields, collection_id),
    }
}

fn validate_conditions(
    conds: &serde_json::Map<String, JsonValue>,
    fields: &[FieldDescription],
    collection_id: &str,
) -> Result<(), String> {
    for (key, val) in conds {
        match key.as_str() {
            "_and" | "_or" => {
                let arr = val.as_array().ok_or_else(|| {
                    format!(
                        "'{key}' in replication filter for collection '{collection_id}' must be an array"
                    )
                })?;
                for elem in arr {
                    let obj = elem.as_object().ok_or_else(|| {
                        format!(
                            "each element of '{key}' in replication filter for collection '{collection_id}' must be an object"
                        )
                    })?;
                    validate_conditions(obj, fields, collection_id)?;
                }
            }
            k if k.starts_with('_') => {
                return Err(format!(
                    "unsupported top-level operator '{key}' in replication filter for collection '{collection_id}'"
                ));
            }
            _ => {
                let field = fields
                    .iter()
                    .find(|f| f.name == *key)
                    .ok_or_else(|| {
                        format!(
                            "replication filter field '{key}' not found in collection '{collection_id}'"
                        )
                    })?;
                if !field.immutable {
                    return Err(format!(
                        "replication filter field '{key}' in collection '{collection_id}' must be marked @immutable"
                    ));
                }
                if field.crdt_type != CType::LwwRegister || !field.kind.is_scalar() {
                    return Err(format!(
                        "replication filter field '{key}' in collection '{collection_id}' must be a scalar LWW field"
                    ));
                }
                let op_obj = val.as_object().ok_or_else(|| {
                    format!("replication filter field '{key}' must map to an operator object")
                })?;
                for (op, opval) in op_obj {
                    validate_op(op, opval, &field.kind, key, collection_id)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_op(
    op: &str,
    opval: &JsonValue,
    kind: &FieldKind,
    field_name: &str,
    collection_id: &str,
) -> Result<(), String> {
    match op {
        "_eq" | "_ne" | "_gt" | "_gte" | "_lt" | "_lte" => {
            if !kind.accepts_filter_value(opval) {
                return Err(format!(
                    "replication filter value for field '{field_name}' in collection '{collection_id}' does not match the field type"
                ));
            }
        }
        "_in" | "_nin" => {
            let arr = opval.as_array().ok_or_else(|| {
                format!(
                    "operator '{op}' for field '{field_name}' in collection '{collection_id}' must be an array"
                )
            })?;
            for v in arr {
                if !kind.accepts_filter_value(v) {
                    return Err(format!(
                        "replication filter value for field '{field_name}' in collection '{collection_id}' does not match the field type"
                    ));
                }
            }
        }
        _ => {
            return Err(format!(
                "operator '{op}' is not supported in replication filters (collection '{collection_id}')"
            ));
        }
    }
    Ok(())
}

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
    use schema::{FieldDescription, FieldKind};
    use serde_json::json;

    fn test_fields() -> Vec<FieldDescription> {
        vec![
            FieldDescription::new("1", "agent_did", FieldKind::string()).as_immutable(),
            FieldDescription::new("2", "kind", FieldKind::string()).as_immutable(),
            FieldDescription::new("3", "body", FieldKind::string()),
        ]
    }

    #[test]
    fn valid_in_on_immutable_field() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"agent_did": {"_in": ["did:a", "did:b"]}})
                .as_object()
                .unwrap()
                .clone(),
        );
        assert!(validate_replication_filter(&fields, "col", &filter).is_ok());
    }

    #[test]
    fn valid_and_of_immutable_field_eqs() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"_and": [
                {"agent_did": {"_eq": "did:a"}},
                {"kind": {"_eq": "post"}}
            ]})
            .as_object()
            .unwrap()
            .clone(),
        );
        assert!(validate_replication_filter(&fields, "col", &filter).is_ok());
    }

    #[test]
    fn or_containing_mutable_field_errors() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"_or": [
                {"agent_did": {"_eq": "did:a"}},
                {"body": {"_eq": "hello"}}
            ]})
            .as_object()
            .unwrap()
            .clone(),
        );
        let err = validate_replication_filter(&fields, "col", &filter).unwrap_err();
        assert!(
            err.contains("immutable"),
            "expected immutable error, got: {err}"
        );
    }

    #[test]
    fn unknown_field_errors() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"nosuchfield": {"_eq": "x"}})
                .as_object()
                .unwrap()
                .clone(),
        );
        let err = validate_replication_filter(&fields, "col", &filter).unwrap_err();
        assert!(
            err.contains("not found"),
            "expected not found error, got: {err}"
        );
    }

    #[test]
    fn unsupported_operator_errors() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"agent_did": {"_like": "%did%"}})
                .as_object()
                .unwrap()
                .clone(),
        );
        let err = validate_replication_filter(&fields, "col", &filter).unwrap_err();
        assert!(
            err.contains("not supported"),
            "expected not supported error, got: {err}"
        );
    }

    #[test]
    fn acp_filter_errors() {
        let fields = test_fields();
        let filter = ReplicationFilter::Acp {
            relation: "owner".to_string(),
        };
        let err = validate_replication_filter(&fields, "col", &filter).unwrap_err();
        assert!(
            err.contains("not yet implemented"),
            "expected not yet implemented error, got: {err}"
        );
    }

    #[test]
    fn type_mismatch_in_array_errors() {
        let fields = test_fields();
        let filter = ReplicationFilter::Predicate(
            json!({"agent_did": {"_in": ["did:a", 42]}})
                .as_object()
                .unwrap()
                .clone(),
        );
        let err = validate_replication_filter(&fields, "col", &filter).unwrap_err();
        assert!(
            err.contains("does not match the field type"),
            "expected type mismatch error, got: {err}"
        );
    }

    // ---- QueryReplicationFilterMatcher tests (kept from original) ----

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
