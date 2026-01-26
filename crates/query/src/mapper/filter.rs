//! Filter types and evaluation for query conditions

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};

/// Filter operators for condition matching
/// Uses Go DefraDB naming conventions for compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal (_eq)
    #[serde(rename = "_eq")]
    Eq,
    /// Not equal (_neq) - Go DefraDB naming
    #[serde(rename = "_neq", alias = "_ne")]
    Ne,
    /// Greater than (_gt)
    #[serde(rename = "_gt")]
    Gt,
    /// Greater than or equal (_geq) - Go DefraDB naming
    #[serde(rename = "_geq", alias = "_gte")]
    Gte,
    /// Less than (_lt)
    #[serde(rename = "_lt")]
    Lt,
    /// Less than or equal (_leq) - Go DefraDB naming
    #[serde(rename = "_leq", alias = "_lte")]
    Lte,
    /// In array (_in)
    #[serde(rename = "_in")]
    In,
    /// Not in array (_nin)
    #[serde(rename = "_nin")]
    Nin,
    /// Pattern match (_like)
    #[serde(rename = "_like")]
    Like,
    /// Negated pattern match (_nlike)
    #[serde(rename = "_nlike")]
    Nlike,
    /// Case-insensitive pattern match (_ilike)
    #[serde(rename = "_ilike")]
    Ilike,
    /// Negated case-insensitive pattern match (_nilike)
    #[serde(rename = "_nilike")]
    Nilike,
    /// Array contains value (_contains)
    #[serde(rename = "_contains")]
    Contains,
    /// Array is contained in given array (_contained_in)
    #[serde(rename = "_contained_in")]
    ContainedIn,
    /// Object/map has key (_has_key)
    #[serde(rename = "_has_key")]
    HasKey,
    /// Logical AND (_and)
    #[serde(rename = "_and")]
    And,
    /// Logical OR (_or)
    #[serde(rename = "_or")]
    Or,
    /// Logical NOT (_not)
    #[serde(rename = "_not")]
    Not,
}

impl FilterOp {
    /// Parse a filter operator from string.
    /// Accepts both Go DefraDB naming (_neq, _geq, _leq) and alternative naming (_ne, _gte, _lte).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "_eq" => Some(Self::Eq),
            "_neq" | "_ne" => Some(Self::Ne),
            "_gt" => Some(Self::Gt),
            "_geq" | "_gte" | "_ge" => Some(Self::Gte),
            "_lt" => Some(Self::Lt),
            "_leq" | "_lte" | "_le" => Some(Self::Lte),
            "_in" => Some(Self::In),
            "_nin" => Some(Self::Nin),
            "_like" => Some(Self::Like),
            "_nlike" => Some(Self::Nlike),
            "_ilike" => Some(Self::Ilike),
            "_nilike" => Some(Self::Nilike),
            "_contains" => Some(Self::Contains),
            "_contained_in" => Some(Self::ContainedIn),
            "_has_key" => Some(Self::HasKey),
            "_and" => Some(Self::And),
            "_or" => Some(Self::Or),
            "_not" => Some(Self::Not),
            _ => None,
        }
    }

    /// Get the string representation (uses Go DefraDB naming)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "_eq",
            Self::Ne => "_neq",
            Self::Gt => "_gt",
            Self::Gte => "_geq",
            Self::Lt => "_lt",
            Self::Lte => "_leq",
            Self::In => "_in",
            Self::Nin => "_nin",
            Self::Like => "_like",
            Self::Nlike => "_nlike",
            Self::Ilike => "_ilike",
            Self::Nilike => "_nilike",
            Self::Contains => "_contains",
            Self::ContainedIn => "_contained_in",
            Self::HasKey => "_has_key",
            Self::And => "_and",
            Self::Or => "_or",
            Self::Not => "_not",
        }
    }

    /// Check if this is a logical operator
    pub fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Not)
    }
}

/// Filter condition containing parsed conditions.
///
/// Conditions map field names to their filter values.
/// Supports nested conditions for related objects.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Parsed filter conditions
    conditions: HashMap<String, JsonValue>,
}

impl Filter {
    /// Create an empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a filter from conditions map
    pub fn from_conditions(conditions: HashMap<String, JsonValue>) -> Self {
        Self { conditions }
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Get a reference to the conditions map
    pub fn conditions(&self) -> &HashMap<String, JsonValue> {
        &self.conditions
    }

    /// Get all field names referenced by this filter
    pub fn referenced_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        Self::collect_fields(&self.conditions, &mut fields);
        fields
    }

    fn collect_fields(conditions: &HashMap<String, JsonValue>, fields: &mut Vec<String>) {
        for (key, value) in conditions {
            // Skip logical operators
            if FilterOp::parse(key).is_some() {
                match value {
                    JsonValue::Array(arr) => {
                        for item in arr {
                            if let JsonValue::Object(obj) = item {
                                let nested: HashMap<String, JsonValue> =
                                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                Self::collect_fields(&nested, fields);
                            }
                        }
                    }
                    JsonValue::Object(obj) => {
                        let nested: HashMap<String, JsonValue> =
                            obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        Self::collect_fields(&nested, fields);
                    }
                    _ => {}
                }
            } else {
                // This is a field name
                if !fields.contains(key) {
                    fields.push(key.clone());
                }
            }
        }
    }

    /// Check if this filter contains relation filters (filters through nested objects).
    ///
    /// Relation filters are conditions like `{author: {verified: {_eq: true}}}` where
    /// the first level field is a relation and the nested object contains field conditions
    /// rather than operators.
    pub fn has_relation_filters(&self) -> bool {
        Self::check_for_relation_filters(&self.conditions)
    }

    /// Get the names of relation fields referenced in this filter.
    ///
    /// Returns field names that have nested object conditions (not operators),
    /// indicating they are relation filters.
    pub fn relation_field_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        Self::collect_relation_field_names(&self.conditions, &mut names);
        names
    }

    fn collect_relation_field_names(conditions: &HashMap<String, JsonValue>, names: &mut Vec<String>) {
        for (key, value) in conditions {
            // Check logical operators recursively
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    Self::collect_relation_field_names(&nested, names);
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            Self::collect_relation_field_names(&nested, names);
                        }
                    }
                    _ => {}
                }
            } else if let JsonValue::Object(obj) = value {
                // This is a field condition - check if it contains operators or nested fields
                // If any key in the object is NOT an operator, it's a relation filter
                let is_relation = obj.keys().any(|k| FilterOp::parse(k).is_none());
                if is_relation && !names.contains(key) {
                    names.push(key.clone());
                }
            }
        }
    }

    fn check_for_relation_filters(conditions: &HashMap<String, JsonValue>) -> bool {
        for (key, value) in conditions {
            // Check logical operators recursively
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    if Self::check_for_relation_filters(&nested) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            if Self::check_for_relation_filters(&nested) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            } else if let JsonValue::Object(obj) = value {
                // This is a field condition - check if it contains operators or nested fields
                // If any key in the object is NOT an operator, it's a relation filter
                for nested_key in obj.keys() {
                    if FilterOp::parse(nested_key).is_none() {
                        // This is a nested field name, not an operator - it's a relation filter
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Get all multi-level relation paths in this filter.
    ///
    /// For a filter like `{author: {published: {rating: {_eq: 4.9}}}}`, this returns
    /// `[["author", "published"]]` - the path through relations (not including the leaf scalar).
    ///
    /// This is used to detect filters that require nested joins beyond a single level.
    pub fn get_multi_level_relation_paths(&self) -> Vec<Vec<String>> {
        let mut paths = Vec::new();
        Self::collect_relation_paths(&self.conditions, &mut Vec::new(), &mut paths);
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
    /// So the path is ["author", "published"].
    ///
    /// This function only saves the deepest path (the full chain of relations).
    fn collect_relation_paths(
        conditions: &HashMap<String, JsonValue>,
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
                                        let nested: HashMap<String, JsonValue> =
                                            obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                        Self::collect_relation_paths(&nested, current_path, paths);
                                    }
                                }
                            }
                        }
                        FilterOp::Not => {
                            if let JsonValue::Object(obj) = value {
                                let nested: HashMap<String, JsonValue> =
                                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                Self::collect_relation_paths(&nested, current_path, paths);
                            }
                        }
                        _ => {}
                    }
                }
                continue;
            }

            // Check if this key maps to a relation (nested object with non-operator keys)
            if let JsonValue::Object(obj) = value {
                let has_non_operator_keys = obj.keys().any(|k| FilterOp::parse(k).is_none());

                if has_non_operator_keys {
                    // This is a relation - add to path and recurse
                    current_path.push(key.clone());

                    let nested: HashMap<String, JsonValue> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                    // Check if nested has any relations (non-operator keys that are objects with non-operator keys)
                    let nested_has_relations = nested.iter().any(|(k, v)| {
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
                        Self::collect_relation_paths(&nested, current_path, paths);
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

        Self::extract_at_path_recursive(&self.conditions, path)
    }

    fn extract_at_path_recursive(
        conditions: &HashMap<String, JsonValue>,
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
                let nested: HashMap<String, JsonValue> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                if remaining_path.is_empty() {
                    // We've reached the end of the path - return this level's conditions
                    return Some(Filter::from_conditions(nested));
                } else {
                    // Continue down the path
                    return Self::extract_at_path_recursive(&nested, remaining_path);
                }
            }
        }

        // Also check inside _and and _or blocks
        for (key, value) in conditions {
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let Some(arr) = value.as_array() {
                            for item in arr {
                                if let Some(obj) = item.as_object() {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    if let Some(filter) =
                                        Self::extract_at_path_recursive(&nested, path)
                                    {
                                        return Some(filter);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        None
    }

    /// Check if this filter is "complex" - contains relation conditions inside logical operators.
    ///
    /// A filter is complex when `_and`, `_or`, or `_not` contains a mix of scalar and relation
    /// conditions. Complex filters cannot be split and must be evaluated as a whole after
    /// the join when the merged document is available.
    ///
    /// Examples:
    /// - `{_and: [{rating: {_ge: 4.0}}, {author: {verified: {_eq: true}}}]}` → COMPLEX
    /// - `{author: {verified: {_eq: true}}}` → NOT COMPLEX (relation at root, no logical wrapper)
    /// - `{_and: [{rating: {_ge: 4.0}}, {age: {_gt: 25}}]}` → NOT COMPLEX (only scalars)
    pub fn is_complex(&self) -> bool {
        Self::check_for_complex_filters(&self.conditions)
    }

    fn check_for_complex_filters(conditions: &HashMap<String, JsonValue>) -> bool {
        for (key, value) in conditions {
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            // Check if this logical block contains any relation filters
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    // If any item in the logical block has a relation filter, it's complex
                                    if Self::check_for_relation_filters(&nested) {
                                        return true;
                                    }
                                    // Also check recursively for nested complex filters
                                    if Self::check_for_complex_filters(&nested) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            if Self::check_for_relation_filters(&nested) {
                                return true;
                            }
                            if Self::check_for_complex_filters(&nested) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        false
    }

    /// Split this filter into scalar filters and relation filters.
    ///
    /// Returns (scalar_filter, relation_filter) where:
    /// - scalar_filter contains conditions on direct fields (no nested relation conditions)
    /// - relation_filter contains conditions that traverse relations
    ///
    /// This is used to apply scalar filters before TypeJoin and relation filters after.
    pub fn split_by_relation(&self) -> (Option<Filter>, Option<Filter>) {
        let mut scalar_conditions = HashMap::new();
        let mut relation_conditions = HashMap::new();

        for (key, value) in &self.conditions {
            // Logical operators need special handling - we can't easily split them
            // For now, if they contain relation filters, put the whole condition in relation_filter
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or | FilterOp::Not => {
                        // Check if this logical block contains relation filters
                        let has_relation = match value {
                            JsonValue::Array(arr) => arr.iter().any(|item| {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    Self::check_for_relation_filters(&nested)
                                } else {
                                    false
                                }
                            }),
                            JsonValue::Object(obj) => {
                                let nested: HashMap<String, JsonValue> =
                                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                Self::check_for_relation_filters(&nested)
                            }
                            _ => false,
                        };
                        if has_relation {
                            relation_conditions.insert(key.clone(), value.clone());
                        } else {
                            scalar_conditions.insert(key.clone(), value.clone());
                        }
                    }
                    _ => {
                        scalar_conditions.insert(key.clone(), value.clone());
                    }
                }
            } else if let JsonValue::Object(obj) = value {
                // Field condition - check if it's a relation filter
                let is_relation = obj.keys().any(|k| FilterOp::parse(k).is_none());
                if is_relation {
                    relation_conditions.insert(key.clone(), value.clone());
                } else {
                    scalar_conditions.insert(key.clone(), value.clone());
                }
            } else {
                scalar_conditions.insert(key.clone(), value.clone());
            }
        }

        let scalar = if scalar_conditions.is_empty() {
            None
        } else {
            Some(Filter::from_conditions(scalar_conditions))
        };

        let relation = if relation_conditions.is_empty() {
            None
        } else {
            Some(Filter::from_conditions(relation_conditions))
        };

        (scalar, relation)
    }

    /// Evaluate the filter against document fields
    pub fn matches(&self, fields: &[Option<JsonValue>], mapping: &DocumentMapping) -> Result<bool> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        self.eval_conditions(&self.conditions, fields, mapping)
    }

    fn eval_conditions(
        &self,
        conditions: &HashMap<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
    ) -> Result<bool> {
        for (key, value) in conditions {
            // Check for logical operators
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if !self.eval_conditions(&sub_conditions, fields, mapping)? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if self.eval_conditions(&sub_conditions, fields, mapping)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        let sub_conditions: HashMap<String, JsonValue> =
                            serde_json::from_value(value.clone())
                                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                        if self.eval_conditions(&sub_conditions, fields, mapping)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {} // Non-logical ops handled below
                }
            }

            // Field condition
            let field_index = mapping
                .first_index_of_name(key)
                .ok_or_else(|| QueryError::unknown_field(key))?;

            let field_value = fields
                .get(field_index)
                .and_then(|v| v.as_ref())
                .cloned()
                .unwrap_or(JsonValue::Null);

            // Value should be an object with operator keys or nested field conditions
            let ops = value
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("field condition must be object"))?;

            // Check if this is a relation filter (nested field conditions) or operator conditions
            let is_relation_filter = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_relation_filter {
                // This is a relation filter like {author: {verified: {_eq: true}}}
                // The field_value should be a JSON object (the related document)
                if field_value.is_null() {
                    // No related document - filter doesn't match
                    return Ok(false);
                }

                let related_obj = field_value.as_object().ok_or_else(|| {
                    QueryError::invalid_filter(format!(
                        "relation field '{}' is not an object",
                        key
                    ))
                })?;

                // Recursively evaluate the nested conditions against the related object
                if !self.eval_relation_conditions(ops, related_obj)? {
                    return Ok(false);
                }
            } else {
                // Standard operator conditions
                for (op_str, expected) in ops {
                    let op = FilterOp::parse(op_str).ok_or_else(|| {
                        QueryError::invalid_filter(format!("unknown operator: {}", op_str))
                    })?;

                    if !self.eval_op(&field_value, op, expected)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Evaluate relation filter conditions against a JSON object.
    ///
    /// This handles nested conditions like `{verified: {_eq: true}}` where the
    /// condition is evaluated against a related document's fields.
    fn eval_relation_conditions(
        &self,
        conditions: &serde_json::Map<String, JsonValue>,
        obj: &serde_json::Map<String, JsonValue>,
    ) -> Result<bool> {
        for (key, value) in conditions {
            // Check for logical operators
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub_conds = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_and items must be objects")
                            })?;
                            if !self.eval_relation_conditions(sub_conds, obj)? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conds = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_or items must be objects")
                            })?;
                            if self.eval_relation_conditions(sub_conds, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        let sub_conds = value.as_object().ok_or_else(|| {
                            QueryError::invalid_filter("_not requires object")
                        })?;
                        if self.eval_relation_conditions(sub_conds, obj)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            // Field condition - get the field value from the object
            let field_value = obj.get(key).cloned().unwrap_or(JsonValue::Null);

            let ops = value
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("field condition must be object"))?;

            // Check if this is another level of nesting
            let is_nested = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_nested {
                // Another level of relation nesting
                if field_value.is_null() {
                    return Ok(false);
                }
                let nested_obj = field_value.as_object().ok_or_else(|| {
                    QueryError::invalid_filter(format!(
                        "nested relation field '{}' is not an object",
                        key
                    ))
                })?;
                if !self.eval_relation_conditions(ops, nested_obj)? {
                    return Ok(false);
                }
            } else {
                // Operator conditions
                for (op_str, expected) in ops {
                    let op = FilterOp::parse(op_str).ok_or_else(|| {
                        QueryError::invalid_filter(format!("unknown operator: {}", op_str))
                    })?;
                    if !self.eval_op(&field_value, op, expected)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn eval_op(&self, actual: &JsonValue, op: FilterOp, expected: &JsonValue) -> Result<bool> {
        match op {
            FilterOp::Eq => Ok(Self::values_equal(actual, expected)),
            FilterOp::Ne => Ok(!Self::values_equal(actual, expected)),
            // Comparison operators: None (from null or NaN) returns false (Go DefraDB behavior)
            FilterOp::Gt => self
                .compare(actual, expected)
                .map(|opt| opt.is_some_and(|ord| ord.is_gt())),
            FilterOp::Gte => self
                .compare(actual, expected)
                .map(|opt| opt.is_some_and(|ord| ord.is_ge())),
            FilterOp::Lt => self
                .compare(actual, expected)
                .map(|opt| opt.is_some_and(|ord| ord.is_lt())),
            FilterOp::Lte => self
                .compare(actual, expected)
                .map(|opt| opt.is_some_and(|ord| ord.is_le())),
            FilterOp::In => {
                let arr = expected
                    .as_array()
                    .ok_or_else(|| QueryError::invalid_filter("_in requires array"))?;
                Ok(arr.iter().any(|v| Self::values_equal(actual, v)))
            }
            FilterOp::Nin => {
                let arr = expected
                    .as_array()
                    .ok_or_else(|| QueryError::invalid_filter("_nin requires array"))?;
                Ok(!arr.iter().any(|v| Self::values_equal(actual, v)))
            }
            FilterOp::Like => self.like_match(actual, expected, false, false),
            FilterOp::Nlike => self.like_match(actual, expected, true, false),
            FilterOp::Ilike => self.like_match(actual, expected, false, true),
            FilterOp::Nilike => self.like_match(actual, expected, true, true),
            FilterOp::Contains => {
                // Array field contains the expected value
                // Null fields never match (standard database behavior)
                if actual.is_null() {
                    return Ok(false);
                }
                let arr = actual
                    .as_array()
                    .ok_or_else(|| QueryError::invalid_filter("_contains requires array field"))?;
                Ok(arr.iter().any(|v| Self::values_equal(v, expected)))
            }
            FilterOp::ContainedIn => {
                // All elements of actual array are in expected array (actual is subset of expected)
                // Null fields never match (standard database behavior)
                if actual.is_null() {
                    return Ok(false);
                }
                let actual_arr = actual.as_array().ok_or_else(|| {
                    QueryError::invalid_filter("_contained_in requires array field")
                })?;
                let expected_arr = expected.as_array().ok_or_else(|| {
                    QueryError::invalid_filter("_contained_in requires array value")
                })?;
                Ok(actual_arr
                    .iter()
                    .all(|v| expected_arr.iter().any(|e| Self::values_equal(v, e))))
            }
            FilterOp::HasKey => {
                // Object/map has the specified key
                // Null fields never match (standard database behavior)
                if actual.is_null() {
                    return Ok(false);
                }
                let key = expected
                    .as_str()
                    .ok_or_else(|| QueryError::invalid_filter("_has_key requires string key"))?;
                let obj = actual
                    .as_object()
                    .ok_or_else(|| QueryError::invalid_filter("_has_key requires object field"))?;
                Ok(obj.contains_key(key))
            }
            FilterOp::And | FilterOp::Or | FilterOp::Not => Err(QueryError::internal(
                "logical ops should be handled at top level",
            )),
        }
    }

    fn values_equal(a: &JsonValue, b: &JsonValue) -> bool {
        match (a, b) {
            (JsonValue::Null, JsonValue::Null) => true,
            (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                // Handle int/float comparison
                if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                    a == b
                } else if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                    (a - b).abs() < f64::EPSILON
                } else {
                    false
                }
            }
            (JsonValue::String(a), JsonValue::String(b)) => a == b,
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| Self::values_equal(a, b))
            }
            _ => false,
        }
    }

    /// Compare two values for ordering.
    /// Returns None for null comparisons (Go DefraDB behavior: null comparisons return false).
    /// Supports int/float coercion (Go DefraDB uses numbers.TryUpcast).
    fn compare(&self, a: &JsonValue, b: &JsonValue) -> Result<Option<std::cmp::Ordering>> {
        match (a, b) {
            // Null comparisons: Go DefraDB returns false for null vs anything in ordering comparisons
            (JsonValue::Null, _) | (_, JsonValue::Null) => Ok(None),

            // Number comparisons: support int/float coercion (Go's numbers.TryUpcast behavior)
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let a_val = a.as_f64().ok_or_else(|| {
                    QueryError::invalid_filter(format!("number {} cannot be compared", a))
                })?;
                let b_val = b.as_f64().ok_or_else(|| {
                    QueryError::invalid_filter(format!("number {} cannot be compared", b))
                })?;
                Ok(a_val.partial_cmp(&b_val)) // Returns None for NaN, which becomes false
            }

            // String comparisons
            (JsonValue::String(a), JsonValue::String(b)) => Ok(Some(a.cmp(b))),

            // Type mismatch
            _ => Err(QueryError::TypeMismatch {
                expected: "comparable types".to_string(),
                actual: format!("{:?} vs {:?}", a, b),
            }),
        }
    }

    fn like_match(
        &self,
        actual: &JsonValue,
        pattern: &JsonValue,
        negate: bool,
        case_insensitive: bool,
    ) -> Result<bool> {
        // Null fields never match (standard database behavior, matches Go DefraDB)
        if actual.is_null() {
            return Ok(negate);
        }

        let op_name = if case_insensitive { "_ilike" } else { "_like" };

        let actual_str = actual.as_str().ok_or_else(|| {
            QueryError::invalid_filter(format!("{} requires string field", op_name))
        })?;
        let pattern_str = pattern.as_str().ok_or_else(|| {
            QueryError::invalid_filter(format!("{} requires string pattern", op_name))
        })?;

        // Apply case transformation if case-insensitive
        let (actual_cmp, pattern_cmp): (std::borrow::Cow<str>, std::borrow::Cow<str>) =
            if case_insensitive {
                (
                    actual_str.to_lowercase().into(),
                    pattern_str.to_lowercase().into(),
                )
            } else {
                (actual_str.into(), pattern_str.into())
            };

        // Pattern matching following Go DefraDB behavior:
        // - 'prefix%' (starts with)
        // - '%suffix' (ends with)
        // - '%contains%' (contains)
        // - 'prefix%suffix' (starts with AND ends with)
        // - 'exact' (exact match)
        // Note: '_' wildcard is treated as literal character (matches Go behavior)
        let matches = if let Some(inner) = pattern_cmp
            .strip_prefix('%')
            .and_then(|s| s.strip_suffix('%'))
        {
            // %contains%
            actual_cmp.contains(inner)
        } else if let Some(suffix) = pattern_cmp.strip_prefix('%') {
            // %suffix (and suffix has no %)
            if suffix.contains('%') {
                // Invalid: multiple % not at edges
                return Err(QueryError::invalid_filter(format!(
                    "{} does not support multiple wildcards except '%contains%'",
                    op_name
                )));
            }
            actual_cmp.ends_with(suffix)
        } else if let Some(prefix) = pattern_cmp.strip_suffix('%') {
            // prefix% (and prefix has no %)
            if prefix.contains('%') {
                // Invalid: multiple % not at edges
                return Err(QueryError::invalid_filter(format!(
                    "{} does not support multiple wildcards except '%contains%'",
                    op_name
                )));
            }
            actual_cmp.starts_with(prefix)
        } else if pattern_cmp.contains('%') {
            // prefix%suffix pattern (single % in the middle)
            let parts: Vec<&str> = pattern_cmp.splitn(2, '%').collect();
            if parts.len() == 2 {
                let prefix = parts[0];
                let suffix = parts[1];
                // Suffix should not contain more %
                if suffix.contains('%') {
                    return Err(QueryError::invalid_filter(format!(
                        "{} does not support multiple wildcards except '%contains%'",
                        op_name
                    )));
                }
                actual_cmp.starts_with(prefix) && actual_cmp.ends_with(suffix)
            } else {
                false
            }
        } else {
            // Exact match
            actual_cmp == pattern_cmp
        };

        Ok(if negate { !matches } else { matches })
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
        if let Some(value) = self.conditions.get(relation_field) {
            if let Some(obj) = value.as_object() {
                // Check if this is a nested filter (has non-operator keys)
                let is_nested = obj.keys().any(|k| FilterOp::parse(k).is_none());
                if is_nested {
                    // Convert the nested object to a Filter
                    let nested_conditions: HashMap<String, JsonValue> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    return Some(Filter::from_conditions(nested_conditions));
                }
            }
        }

        // Also check inside _and and _or blocks
        for (key, value) in &self.conditions {
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let Some(arr) = value.as_array() {
                            for item in arr {
                                if let Some(obj) = item.as_object() {
                                    if let Some(rel_value) = obj.get(relation_field) {
                                        if let Some(rel_obj) = rel_value.as_object() {
                                            let is_nested =
                                                rel_obj.keys().any(|k| FilterOp::parse(k).is_none());
                                            if is_nested {
                                                let nested_conditions: HashMap<String, JsonValue> =
                                                    rel_obj
                                                        .iter()
                                                        .map(|(k, v)| (k.clone(), v.clone()))
                                                        .collect();
                                                return Some(Filter::from_conditions(
                                                    nested_conditions,
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "age");
        m.add(3, "active");
        m
    }

    fn make_fields() -> Vec<Option<JsonValue>> {
        vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            Some(json!(30)),
            Some(json!(true)),
        ]
    }

    #[test]
    fn test_empty_filter_matches_all() {
        let filter = Filter::new();
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_eq_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Alice"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_eq": "Bob"}))]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ne_filter() {
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_ne": "Bob"}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_gt_filter() {
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 35}))]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_in_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_in": ["Alice", "Bob", "Charlie"]}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_in": ["Bob", "Charlie"]}),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_and_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"name": {"_eq": "Alice"}},
                {"age": {"_gte": 18}}
            ]),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"name": {"_eq": "Alice"}},
                {"age": {"_lt": 18}}
            ]),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_or_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "_or".to_string(),
            json!([
                {"name": {"_eq": "Bob"}},
                {"age": {"_eq": 30}}
            ]),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "_or".to_string(),
            json!([
                {"name": {"_eq": "Bob"}},
                {"age": {"_eq": 25}}
            ]),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_not_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "_not".to_string(),
            json!({"name": {"_eq": "Bob"}}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "%ice"}),
        )]));
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_prefix() {
        // Pattern "ALI%" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALI%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_suffix() {
        // Pattern "%ICE" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "%ICE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_contains() {
        // Pattern "%LIC%" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "%LIC%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_exact() {
        // Pattern "ALICE" should match "Alice" (case-insensitive exact)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALICE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_no_match() {
        // Pattern "BOB%" should NOT match "Alice"
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "BOB%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_nilike_filter() {
        // Negated: pattern "BOB%" should NOT match "Alice", so nilike returns true
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "BOB%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        // Negated: pattern "ALI%" WOULD match "Alice", so nilike returns false
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "ALI%"}),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_underscore_as_literal() {
        // Underscore is treated as literal character (matches Go behavior)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Al_ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
                                    // "Al_ce" should NOT match "Alice" because _ is literal, not wildcard
        assert!(!filter.matches(&fields, &mapping).unwrap());

        // But "Al_ce" should match "Al_ce" exactly
        let mut fields_with_underscore = make_fields();
        fields_with_underscore[1] = Some(json!("Al_ce"));
        assert!(filter.matches(&fields_with_underscore, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_prefix_suffix_pattern() {
        // Pattern "Ali%ce" should match "Alice" (starts with "ali" AND ends with "ce")
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALI%CE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_prefix_suffix_pattern() {
        // Pattern "Ali%ce" should match "Alice" (case-sensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Ali%ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());

        // Wrong case should NOT match
        let filter_wrong_case = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "ALI%CE"}),
        )]));
        assert!(!filter_wrong_case.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_prefix_suffix_no_match() {
        // Pattern "Bob%son" should NOT match "Alice"
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Bob%son"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_null_field_returns_false() {
        // Null field should return false for _ilike (not error), matching Go behavior
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!(null)); // name is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_nilike_null_field_returns_true() {
        // Negated: null field with _nilike should return true (null doesn't match pattern)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!(null)); // name is null
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    // Helper to create mapping with array and object fields for testing
    fn make_extended_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "age");
        m.add(3, "active");
        m.add(4, "tags"); // Array field
        m.add(5, "metadata"); // Object field
        m
    }

    fn make_extended_fields() -> Vec<Option<JsonValue>> {
        vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            Some(json!(30)),
            Some(json!(true)),
            Some(json!(["rust", "database", "graphql"])), // tags array
            Some(json!({"version": "1.0", "author": "Alice"})), // metadata object
        ]
    }

    #[test]
    fn test_contains_filter_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_filter_no_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "python"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_filter_non_array_error() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array field"));
    }

    #[test]
    fn test_contained_in_filter_match() {
        // All elements of tags are in the given array
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": ["rust", "database", "graphql", "sql", "nosql"]}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_filter_no_match() {
        // Not all elements of tags are in the given array (missing "graphql")
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": ["rust", "database"]}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_filter_empty_field_array() {
        // Empty array is contained in any array
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": ["anything"]}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!([])); // Empty tags array
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_filter_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": "version"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_filter_no_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": "nonexistent"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_filter_non_object_error() {
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_has_key": "version"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires object field"));
    }

    #[test]
    fn test_filter_op_parse() {
        assert_eq!(FilterOp::parse("_eq"), Some(FilterOp::Eq));
        assert_eq!(FilterOp::parse("_and"), Some(FilterOp::And));
        assert_eq!(FilterOp::parse("_ilike"), Some(FilterOp::Ilike));
        assert_eq!(FilterOp::parse("_nilike"), Some(FilterOp::Nilike));
        assert_eq!(FilterOp::parse("_contains"), Some(FilterOp::Contains));
        assert_eq!(
            FilterOp::parse("_contained_in"),
            Some(FilterOp::ContainedIn)
        );
        assert_eq!(FilterOp::parse("_has_key"), Some(FilterOp::HasKey));
        assert_eq!(FilterOp::parse("invalid"), None);
    }

    #[test]
    fn test_null_field_comparison() {
        // When a field is null/None, comparisons should handle it gracefully
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_eq": null}))]));
        let mapping = make_mapping();
        // Field at index 2 (age) is None
        let fields = vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            None, // age is null
            Some(json!(true)),
        ];
        // Null equals null
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_null_field_gt_comparison_returns_false() {
        // Go DefraDB behavior: null _gt 25 returns false (not error)
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));
        let mapping = make_mapping();
        let fields = vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            None, // age is null
            Some(json!(true)),
        ];
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_ok(), "null _gt comparison should not error");
        assert!(!result.unwrap(), "null _gt 25 should return false");
    }

    #[test]
    fn test_nested_and_or_operators() {
        // Test _and containing _or: match if (name=Alice OR name=Bob) AND age>=18
        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"_or": [
                    {"name": {"_eq": "Alice"}},
                    {"name": {"_eq": "Bob"}}
                ]},
                {"age": {"_gte": 18}}
            ]),
        )]));
        let mapping = make_mapping();

        // Alice, age 30 - should match
        let fields_alice = vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            Some(json!(30)),
            Some(json!(true)),
        ];
        assert!(filter.matches(&fields_alice, &mapping).unwrap());

        // Charlie, age 25 - should NOT match (name not Alice or Bob)
        let fields_charlie = vec![
            Some(json!("doc2")),
            Some(json!("Charlie")),
            Some(json!(25)),
            Some(json!(true)),
        ];
        assert!(!filter.matches(&fields_charlie, &mapping).unwrap());
    }

    #[test]
    fn test_nested_not_and_operators() {
        // Test _not containing _and: match if NOT (name=Alice AND age<18)
        let filter = Filter::from_conditions(HashMap::from([(
            "_not".to_string(),
            json!({"_and": [
                {"name": {"_eq": "Alice"}},
                {"age": {"_lt": 18}}
            ]}),
        )]));
        let mapping = make_mapping();

        // Alice, age 30 - should match (Alice but NOT age<18)
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        // Alice, age 15 - should NOT match
        let fields_young = vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            Some(json!(15)),
            Some(json!(true)),
        ];
        assert!(!filter.matches(&fields_young, &mapping).unwrap());
    }

    #[test]
    fn test_like_underscore_as_literal() {
        // Underscore is treated as literal character (matches Go behavior)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Al_ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        // "Al_ce" should NOT match "Alice" because _ is literal
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_unsupported_complex_pattern() {
        // Multiple % not at edges should error (except %contains%)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "%li%ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_err());
    }

    // =========================================================================
    // Null field handling tests for array/object operators
    // =========================================================================

    #[test]
    fn test_contains_null_field_returns_false() {
        // When field is null, _contains should return false (not error)
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!(null)); // tags is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_null_field_returns_false() {
        // When field is null, _contained_in should return false (not error)
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": ["rust", "go"]}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!(null)); // tags is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_null_field_returns_false() {
        // When field is null, _has_key should return false (not error)
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": "version"}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[5] = Some(json!(null)); // metadata is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_with_null_in_array() {
        // Array contains null, searching for null should find it
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": null}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!(["rust", null, "graphql"])); // Array with null
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_null_not_in_array() {
        // Array doesn't contain null, searching for null should not find it
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": null}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields(); // No null in tags
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    // =========================================================================
    // Empty array edge cases
    // =========================================================================

    #[test]
    fn test_contains_empty_array() {
        // Empty array should never contain anything
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!([])); // Empty array
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_empty_expected_array() {
        // Non-empty field array vs empty expected array
        // [a,b,c] is NOT contained in [] (no elements of expected contain the actuals)
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": []}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields(); // tags = ["rust", "database", "graphql"]
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_empty_string_key() {
        // Empty string keys are valid in JSON objects
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": ""}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[5] = Some(json!({"": "empty key value", "version": "1.0"}));
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    // =========================================================================
    // Pattern matching edge cases
    // =========================================================================

    #[test]
    fn test_like_pattern_only_percent() {
        // Pattern "%" should match any non-empty string (suffix after empty prefix)
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": "%"}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_empty_pattern() {
        // Empty pattern should only match empty string
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": ""}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_empty_pattern_matches_empty_string() {
        // Empty pattern should match empty string
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": ""}))]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!("")); // empty name
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    // Tests for is_complex()
    #[test]
    fn test_is_complex_simple_scalar() {
        // Simple scalar filter is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Alice"}),
        )]));
        assert!(!filter.is_complex());
    }

    #[test]
    fn test_is_complex_simple_relation_at_root() {
        // Relation filter at root level (no logical wrapper) is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        assert!(!filter.is_complex());
    }

    #[test]
    fn test_is_complex_and_with_only_scalars() {
        // _and with only scalar conditions is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"name": {"_eq": "Alice"}},
                {"age": {"_gt": 25}}
            ]),
        )]));
        assert!(!filter.is_complex());
    }

    #[test]
    fn test_is_complex_and_with_relation() {
        // _and containing a relation filter IS complex
        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"rating": {"_ge": 4.0}},
                {"author": {"verified": {"_eq": true}}}
            ]),
        )]));
        assert!(filter.is_complex());
    }

    #[test]
    fn test_is_complex_or_with_relation() {
        // _or containing a relation filter IS complex
        let filter = Filter::from_conditions(HashMap::from([(
            "_or".to_string(),
            json!([
                {"rating": {"_ge": 4.0}},
                {"author": {"verified": {"_eq": true}}}
            ]),
        )]));
        assert!(filter.is_complex());
    }

    #[test]
    fn test_is_complex_not_with_relation() {
        // _not containing a relation filter IS complex
        let filter = Filter::from_conditions(HashMap::from([(
            "_not".to_string(),
            json!({"author": {"verified": {"_eq": true}}}),
        )]));
        assert!(filter.is_complex());
    }

    // =========================================================================
    // Multi-level relation path detection tests
    // =========================================================================

    #[test]
    fn test_get_multi_level_relation_paths_simple_relation() {
        // Single-level relation like {author: {verified: {_eq: true}}}
        // Path is ["author"] which has length 1, so should NOT be in multi-level paths
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        let paths = filter.get_multi_level_relation_paths();
        assert!(paths.is_empty(), "Single-level relation should not return multi-level paths");
    }

    #[test]
    fn test_get_multi_level_relation_paths_two_level() {
        // Two-level relation like {author: {published: {rating: {_eq: 4.9}}}}
        // Path is ["author", "published"] which has length 2
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"published": {"rating": {"_eq": 4.9}}}),
        )]));
        let paths = filter.get_multi_level_relation_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec!["author".to_string(), "published".to_string()]);
    }

    #[test]
    fn test_get_multi_level_relation_paths_three_level() {
        // Three-level relation like {author: {publisher: {country: {name: {_eq: "USA"}}}}}
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"publisher": {"country": {"name": {"_eq": "USA"}}}}),
        )]));
        let paths = filter.get_multi_level_relation_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![
            "author".to_string(),
            "publisher".to_string(),
            "country".to_string()
        ]);
    }

    #[test]
    fn test_get_multi_level_relation_paths_no_relation() {
        // Scalar filter, no relations
        let filter = Filter::from_conditions(HashMap::from([(
            "rating".to_string(),
            json!({"_eq": 4.9}),
        )]));
        let paths = filter.get_multi_level_relation_paths();
        assert!(paths.is_empty());
    }

    #[test]
    fn test_extract_filter_at_path_single_level() {
        // Extract filter at ["author"] from {author: {verified: {_eq: true}}}
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        let extracted = filter.extract_filter_at_path(&["author".to_string()]);
        assert!(extracted.is_some());
        let extracted = extracted.unwrap();
        // Should contain {verified: {_eq: true}}
        assert!(extracted.conditions().contains_key("verified"));
    }

    #[test]
    fn test_extract_filter_at_path_two_level() {
        // Extract filter at ["author", "published"] from {author: {published: {rating: {_eq: 4.9}}}}
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"published": {"rating": {"_eq": 4.9}}}),
        )]));
        let extracted = filter.extract_filter_at_path(&[
            "author".to_string(),
            "published".to_string(),
        ]);
        assert!(extracted.is_some());
        let extracted = extracted.unwrap();
        // Should contain {rating: {_eq: 4.9}}
        assert!(extracted.conditions().contains_key("rating"));
    }

    #[test]
    fn test_extract_filter_at_path_empty_path() {
        // Empty path should return the full filter
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        let extracted = filter.extract_filter_at_path(&[]);
        assert!(extracted.is_some());
        let extracted = extracted.unwrap();
        assert!(extracted.conditions().contains_key("author"));
    }

    #[test]
    fn test_extract_filter_at_path_nonexistent() {
        // Path that doesn't exist should return None
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        let extracted = filter.extract_filter_at_path(&["nonexistent".to_string()]);
        assert!(extracted.is_none());
    }
}
