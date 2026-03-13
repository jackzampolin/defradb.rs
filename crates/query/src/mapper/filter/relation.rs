//! Relation filter utilities - extraction and path analysis

use serde_json::{Map, Value as JsonValue};

use super::filter_impl::Filter;
use super::op::FilterOp;

impl Filter {
    /// Get the names of relation fields referenced in this filter.
    ///
    /// Returns field names that have nested object conditions (not operators),
    /// indicating they are relation filters.
    pub fn relation_field_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        Self::collect_relation_field_names(self.conditions(), &mut names);
        names
    }

    fn collect_relation_field_names(conditions: &Map<String, JsonValue>, names: &mut Vec<String>) {
        for (key, value) in conditions {
            // Check logical operators recursively
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    Self::collect_relation_field_names(obj, names);
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            Self::collect_relation_field_names(obj, names);
                        }
                    }
                    _ => {}
                }
            } else if key == "_alias" {
                // _alias is a special filter directive, not a relation field
                continue;
            } else if let JsonValue::Object(obj) = value {
                // Skip fields where _similarity is used as a nested key —
                // it's a select-field directive, not a relation indicator
                if obj.contains_key("SIMILARITY") {
                    continue;
                }
                // This is a field condition - check if it contains operators or nested fields
                // If any key in the object is NOT an operator, it's a relation filter
                let is_relation = obj.keys().any(|k| FilterOp::parse(k).is_none());
                if is_relation && !names.contains(key) {
                    names.push(key.clone());
                }
            }
        }
    }

    /// Get all relation filter conditions.
    ///
    /// Returns an iterator over (relation_name, nested_filter) pairs for each relation
    /// referenced in the filter.
    ///
    /// For a filter like `{author: {verified: {_eq: true}}, rating: {_gt: 4}}`:
    /// - Returns: [("author", Filter({verified: {_eq: true}}))]
    /// - "rating" is not a relation filter (it has operators, not nested fields)
    pub fn relation_conditions(&self) -> Vec<(String, Filter)> {
        let mut result = Vec::new();
        for name in self.relation_field_names() {
            if let Some(filter) = self.extract_relation_filter(&name) {
                result.push((name, filter));
            }
        }
        result
    }

    /// Extract the filter conditions for a specific relation field.
    ///
    /// For a filter like `{author: {verified: {_eq: true}}}`, calling
    /// `extract_relation_filter("author")` returns a Filter with conditions
    /// `{verified: {_eq: true}}`.
    ///
    /// Returns None if there's no filter condition for this relation field.
    pub fn extract_relation_filter(&self, relation_field: &str) -> Option<Filter> {
        // Check if this relation field has conditions at the top level
        if let Some(value) = self.conditions().get(relation_field) {
            if let Some(obj) = value.as_object() {
                // Check if this is a nested filter (has non-operator keys)
                let is_nested = obj.keys().any(|k| FilterOp::parse(k).is_none());
                if is_nested {
                    return Some(Filter::from_conditions(obj.clone()));
                }
            }
        }

        // Also check inside _and and _or blocks
        for (key, value) in self.conditions() {
            if let Some(FilterOp::And | FilterOp::Or) = FilterOp::parse(key) {
                if let Some(arr) = value.as_array() {
                    for item in arr {
                        if let Some(obj) = item.as_object() {
                            if let Some(rel_value) = obj.get(relation_field) {
                                if let Some(rel_obj) = rel_value.as_object() {
                                    let is_nested =
                                        rel_obj.keys().any(|k| FilterOp::parse(k).is_none());
                                    if is_nested {
                                        return Some(Filter::from_conditions(rel_obj.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        None
    }

    /// Get all multi-level relation paths in this filter.
    ///
    /// For a filter like `{author: {published: {rating: {_eq: 4.9}}}}`, this returns
    /// `[["author", "published"]]` - the path through relations (not including the leaf scalar).
    ///
    /// This is used to detect filters that require nested joins beyond a single level.
    pub fn get_multi_level_relation_paths(&self) -> Vec<Vec<String>> {
        let mut paths = Vec::new();
        Self::collect_relation_paths(self.conditions(), &mut Vec::new(), &mut paths);
        // Only return paths with more than one element (multi-level)
        paths.into_iter().filter(|p| p.len() > 1).collect()
    }

    /// Recursively collect relation paths from filter conditions.
    ///
    /// A relation path is a sequence of nested field names before reaching a scalar condition.
    /// For `{author: {published: {rating: {_eq: 4.9}}}}`:
    /// - "author" is a relation (its value has non-operator keys)
    /// - "published" is a relation (its value has non-operator keys)
    /// - "rating" is a scalar (its value only has operator keys like "_eq")
    ///   So the path is ["author", "published"].
    fn collect_relation_paths(
        conditions: &Map<String, JsonValue>,
        current_path: &mut Vec<String>,
        paths: &mut Vec<Vec<String>>,
    ) {
        for (key, value) in conditions {
            // Skip logical operators at this level
            if FilterOp::parse(key).is_some() {
                // For _and/_or/_not, recurse into their contents
                if let Some(op) = FilterOp::parse(key) {
                    match op {
                        FilterOp::And | FilterOp::Or => {
                            if let JsonValue::Array(arr) = value {
                                for item in arr {
                                    if let JsonValue::Object(obj) = item {
                                        Self::collect_relation_paths(obj, current_path, paths);
                                    }
                                }
                            }
                        }
                        FilterOp::Not => {
                            if let JsonValue::Object(obj) = value {
                                Self::collect_relation_paths(obj, current_path, paths);
                            }
                        }
                        _ => {}
                    }
                }
                continue;
            }

            // _alias is a special filter directive, not a relation path
            if key == "_alias" {
                continue;
            }

            // Check if this key maps to a relation (nested object with non-operator keys)
            if let JsonValue::Object(obj) = value {
                let has_non_operator_keys = obj.keys().any(|k| FilterOp::parse(k).is_none());

                if has_non_operator_keys {
                    // This is a relation - add to path and recurse
                    current_path.push(key.clone());

                    // Check if nested has any relations (non-operator keys that are objects with non-operator keys)
                    let nested_has_relations = obj.iter().any(|(k, v)| {
                        if FilterOp::parse(k).is_some() {
                            return false;
                        }
                        if let JsonValue::Object(inner) = v {
                            inner.keys().any(|ik| FilterOp::parse(ik).is_none())
                        } else {
                            false
                        }
                    });

                    if nested_has_relations {
                        // Continue recursing - there are more relations
                        Self::collect_relation_paths(obj, current_path, paths);
                    } else {
                        // This is the deepest level - save the path
                        paths.push(current_path.clone());
                    }

                    current_path.pop();
                }
            }
        }
    }

    /// Extract filter conditions at a specific relation path.
    ///
    /// For a filter like `{author: {published: {rating: {_eq: 4.9}}}}` and path `["author"]`,
    /// this returns a Filter with conditions `{published: {rating: {_eq: 4.9}}}`.
    ///
    /// For path `["author", "published"]`, returns `{rating: {_eq: 4.9}}`.
    pub fn extract_filter_at_path(&self, path: &[String]) -> Option<Filter> {
        if path.is_empty() {
            return Some(self.clone());
        }

        Self::extract_at_path_recursive(self.conditions(), path)
    }

    fn extract_at_path_recursive(
        conditions: &Map<String, JsonValue>,
        path: &[String],
    ) -> Option<Filter> {
        if path.is_empty() {
            return Some(Filter::from_conditions(conditions.clone()));
        }

        let current_key = &path[0];
        let remaining_path = &path[1..];

        // Look for the key at this level
        if let Some(value) = conditions.get(current_key) {
            if let Some(obj) = value.as_object() {
                if remaining_path.is_empty() {
                    // We've reached the end of the path - return this level's conditions
                    return Some(Filter::from_conditions(obj.clone()));
                } else {
                    // Continue down the path
                    return Self::extract_at_path_recursive(obj, remaining_path);
                }
            }
        }

        // Also check inside _and and _or blocks
        for (key, value) in conditions {
            if let Some(FilterOp::And | FilterOp::Or) = FilterOp::parse(key) {
                if let Some(arr) = value.as_array() {
                    for item in arr {
                        if let Some(obj) = item.as_object() {
                            if let Some(filter) = Self::extract_at_path_recursive(obj, path) {
                                return Some(filter);
                            }
                        }
                    }
                }
            }
        }

        None
    }
}
