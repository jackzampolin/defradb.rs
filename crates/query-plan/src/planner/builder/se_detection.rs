//! SE (Searchable Encryption) detection for the query planner.
//!
//! Detects when query filter conditions reference fields that have encrypted
//! indexes, and builds SE filter conditions for the SEFilterNode.

use schema::{CollectionVersion, EncryptedIndexDescription};
use serde_json::Value as JsonValue;

use crate::plan::SEFilterCondition;
use query_types::document::DocumentMapping;

/// Check if any filter conditions reference encrypted-indexed fields.
///
/// Returns SE filter conditions for fields that have encrypted indexes
/// and are used with equality operators in the filter.
pub fn detect_se_filter_conditions(
    filter: &query_types::mapper::Filter,
    collection: &CollectionVersion,
    mapping: &DocumentMapping,
) -> Vec<SEFilterCondition> {
    if collection.encrypted_indexes.is_empty() {
        return Vec::new();
    }

    let mut conditions = Vec::new();

    for (field_name, value) in filter.conditions() {
        // Skip logical operators
        if field_name.starts_with('_') {
            continue;
        }

        let enc_idx = match find_encrypted_index(&collection.encrypted_indexes, field_name) {
            Some(idx) => idx,
            None => continue,
        };

        let field_index = match mapping.first_index_of_name(field_name) {
            Some(idx) => idx,
            None => continue,
        };

        // Extract equality value from the filter condition.
        // SE only supports equality queries: {field: {_eq: value}}
        if let Some(eq_value) = extract_equality_value(value) {
            conditions.push(SEFilterCondition {
                field_name: field_name.clone(),
                index_desc: enc_idx.clone(),
                field_index,
                filter_value: eq_value,
            });
        }
    }

    conditions
}

/// Find an encrypted index for a given field name.
fn find_encrypted_index<'a>(
    indexes: &'a [EncryptedIndexDescription],
    field_name: &str,
) -> Option<&'a EncryptedIndexDescription> {
    indexes.iter().find(|idx| idx.field_name == field_name)
}

/// Extract the equality value from a filter condition object.
///
/// Returns `Some(value)` if the condition is `{_eq: value}`, None otherwise.
fn extract_equality_value(condition: &JsonValue) -> Option<JsonValue> {
    let obj = condition.as_object()?;
    obj.get("_eq").cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_equality_value() {
        let cond = serde_json::json!({"_eq": 25});
        assert_eq!(extract_equality_value(&cond), Some(serde_json::json!(25)));

        let cond = serde_json::json!({"_gt": 10});
        assert_eq!(extract_equality_value(&cond), None);

        let cond = serde_json::json!({"_eq": "hello"});
        assert_eq!(
            extract_equality_value(&cond),
            Some(serde_json::json!("hello"))
        );
    }

    #[test]
    fn test_find_encrypted_index() {
        let indexes = vec![
            EncryptedIndexDescription::new("age"),
            EncryptedIndexDescription::new("ssn"),
        ];

        assert!(find_encrypted_index(&indexes, "age").is_some());
        assert!(find_encrypted_index(&indexes, "ssn").is_some());
        assert!(find_encrypted_index(&indexes, "name").is_none());
    }
}
