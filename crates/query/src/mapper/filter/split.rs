//! Filter splitting utilities - decompose filters for query planning

use serde_json::{Map, Value as JsonValue};

use super::filter_impl::Filter;
use super::op::FilterOp;

impl Filter {
    /// Split this filter into scalar filters and relation filters.
    ///
    /// Returns (scalar_filter, relation_filter) where:
    /// - scalar_filter contains conditions on direct fields (no nested relation conditions)
    /// - relation_filter contains conditions that traverse relations
    ///
    /// This is used to apply scalar filters before TypeJoin and relation filters after.
    pub fn split_by_relation(&self) -> (Option<Filter>, Option<Filter>) {
        let mut scalar_conditions = Map::new();
        let mut relation_conditions = Map::new();

        for (key, value) in self.conditions() {
            // Logical operators need special handling - we can't easily split them
            // For now, if they contain relation filters, put the whole condition in relation_filter
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or | FilterOp::Not => {
                        // Check if this logical block contains relation filters
                        let has_relation = match value {
                            JsonValue::Array(arr) => arr.iter().any(|item| {
                                item.as_object()
                                    .map(Self::check_for_relation_filters)
                                    .unwrap_or(false)
                            }),
                            JsonValue::Object(obj) => Self::check_for_relation_filters(obj),
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
            } else if key == "_alias" {
                // _alias is a special filter directive evaluated by eval_conditions, not a relation
                scalar_conditions.insert(key.clone(), value.clone());
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
            Some(Filter::from_conditions_with_max_depth(
                scalar_conditions,
                self.max_depth(),
            ))
        };

        let relation = if relation_conditions.is_empty() {
            None
        } else {
            Some(Filter::from_conditions_with_max_depth(
                relation_conditions,
                self.max_depth(),
            ))
        };

        (scalar, relation)
    }

    /// Split the filter into non-alias and alias parts.
    ///
    /// Returns (non_alias_filter, alias_filter) where:
    /// - non_alias_filter contains all conditions except `_alias`
    /// - alias_filter contains only the `_alias` condition
    ///
    /// This is used to apply alias filters after aggregation in grouped queries,
    /// since alias filters on aggregate fields can only be evaluated after the
    /// aggregate values have been computed.
    pub fn split_alias(&self) -> (Option<Filter>, Option<Filter>) {
        let mut non_alias = Map::new();
        let mut alias_only = Map::new();

        for (key, value) in self.conditions() {
            if key == "_alias" {
                alias_only.insert(key.clone(), value.clone());
            } else {
                non_alias.insert(key.clone(), value.clone());
            }
        }

        let non_alias_filter = if non_alias.is_empty() {
            None
        } else {
            Some(Filter::from_conditions_with_max_depth(
                non_alias,
                self.max_depth(),
            ))
        };

        let alias_filter = if alias_only.is_empty() {
            None
        } else {
            Some(Filter::from_conditions_with_max_depth(
                alias_only,
                self.max_depth(),
            ))
        };

        (non_alias_filter, alias_filter)
    }

    /// Strip _alias conditions from the filter that reference aggregate field names.
    ///
    /// This is used to prevent _alias conditions on computed aggregates from being
    /// evaluated during plan execution (when aggregate values don't exist yet).
    /// The stripped conditions should be applied in post-processing after aggregates are computed.
    ///
    /// Returns (filtered, has_aggregate_alias) where:
    /// - filtered: Filter without _alias conditions referencing the given aggregate names
    /// - has_aggregate_alias: true if any _alias conditions were referencing aggregates
    pub fn strip_aggregate_alias_conditions(&self, aggregate_names: &[&str]) -> (Filter, bool) {
        let mut new_conditions = Map::new();
        let mut has_aggregate_alias = false;

        for (key, value) in self.conditions() {
            if key == "_alias" {
                // Check if this _alias block references any aggregate names
                if let Some(alias_obj) = value.as_object() {
                    let refs_aggregate = alias_obj
                        .keys()
                        .any(|k| aggregate_names.contains(&k.as_str()));
                    if refs_aggregate {
                        has_aggregate_alias = true;
                        // Skip this _alias condition - it will be applied in post-processing
                        continue;
                    }
                }
            }
            new_conditions.insert(key.clone(), value.clone());
        }

        (
            Filter::from_conditions_with_max_depth(new_conditions, self.max_depth()),
            has_aggregate_alias,
        )
    }
}
