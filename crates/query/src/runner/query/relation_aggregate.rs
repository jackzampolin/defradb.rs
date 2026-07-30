//! Relation-based aggregate computation.

use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::txn::TransactionRegistry;

use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Compute aggregate values from joined relation data.
    ///
    /// For each relation-based aggregate (e.g., _count(books: {})), this function:
    /// 1. Finds the joined relation data (stored under the relation field name)
    /// 2. Computes the aggregate (count, sum, avg, etc.)
    /// 3. Stores the result under the aggregate's output name
    pub(crate) fn compute_relation_aggregates(
        &self,
        mut results: Vec<JsonValue>,
        select: &Select,
        aggregate_internal_keys: &std::collections::HashMap<String, (String, String)>,
    ) -> Result<Vec<JsonValue>> {
        use crate::mapper::AggregateType;

        // Collect info about relation aggregates with full target references
        let mut aggregates_info: Vec<(
            String,
            AggregateType,
            Vec<&crate::mapper::AggregateTarget>,
        )> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let mut relation_targets = Vec::new();
                for target in &agg.targets {
                    // Skip _group targets - they're handled by GroupByNode and aggregate nodes
                    if !target.host_name.is_empty() && target.host_name != "GROUP" {
                        relation_targets.push(target);
                    }
                }
                if !relation_targets.is_empty() {
                    aggregates_info.push((
                        agg.output_name().to_string(),
                        agg.aggregate_type,
                        relation_targets,
                    ));
                }
            }
        }

        if aggregates_info.is_empty() {
            return Ok(results);
        }

        // Collect which relation fields are explicitly selected and their requested fields (for cleanup later)
        let _selected_relations: std::collections::HashSet<String> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    Some(s.field.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // For each selected relation, collect the fields that were explicitly requested.
        // Any fields NOT in this set were added for aggregate filter evaluation and should be cleaned up.
        let selected_relation_fields: std::collections::HashMap<
            String,
            std::collections::HashSet<String>,
        > = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let mut fields = std::collections::HashSet::new();
                    // Always include _docID as it's implicit
                    fields.insert("_docID".to_string());
                    for requestable in &s.fields {
                        match requestable {
                            Requestable::Field(f) => {
                                fields.insert(f.output_name().to_string());
                            }
                            Requestable::Select(nested) => {
                                fields.insert(nested.field.output_name().to_string());
                            }
                            Requestable::Aggregate(agg) => {
                                fields.insert(agg.output_name().to_string());
                            }
                            Requestable::Similarity(sim) => {
                                fields.insert(sim.output_name().to_string());
                            }
                            Requestable::FullTextSearch(fts) => {
                                fields.insert(fts.output_name().to_string());
                            }
                        }
                    }
                    Some((s.field.output_name().to_string(), fields))
                } else {
                    None
                }
            })
            .collect();

        // Collect all relation names used by aggregates (for deferred cleanup)
        let mut aggregate_relation_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_, _, targets) in &aggregates_info {
            for target in targets {
                aggregate_relation_names.insert(target.host_name.clone());
            }
        }

        // Build a mapping from relation field name → output name for aliased relation selections.
        // When a query uses `NewestPublishersBook: book(...)`, the JSON key is "NewestPublishersBook"
        // but the aggregate target references "book". We need to resolve these aliases.
        let relation_alias_map: std::collections::HashMap<&str, &str> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let name = s.field.name.as_str();
                    let output = s.field.output_name();
                    if name != output {
                        Some((name, output))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Process each result
        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (output_name, agg_type, targets) in &aggregates_info {
                    let mut total_value: f64 = 0.0;
                    let mut total_count: i64 = 0;

                    for target in targets {
                        let relation_name = &target.host_name;
                        let field_name = target.field_name.as_deref();

                        // Try internal key first (when selection and aggregate use same relation),
                        // then direct relation name, then fall back to alias
                        let relation_data = aggregate_internal_keys
                            .get(output_name)
                            .and_then(|(_, internal_key)| obj.get(internal_key.as_str()))
                            .or_else(|| obj.get(relation_name.as_str()))
                            .or_else(|| {
                                relation_alias_map
                                    .get(relation_name.as_str())
                                    .and_then(|alias| obj.get(*alias))
                            });
                        if let Some(relation_data) = relation_data {
                            if let JsonValue::Array(items) = relation_data {
                                // Array data: relation or inline array aggregate
                                // Step 1: Apply filter to array elements
                                let filtered_items: Vec<&JsonValue> = if let Some(ref filter) =
                                    target.filter
                                {
                                    // Check if the filter has field-based conditions
                                    // (keys that are not operators like _gt, _eq, etc.)
                                    let has_field_conditions = filter.has_field_conditions();

                                    items
                                        .iter()
                                        .filter(|item| {
                                            if has_field_conditions {
                                                // Field-based filter like {rating: {_gt: 4.8}}
                                                // Match against the entire item object
                                                filter.matches_json_object(item).unwrap_or(false)
                                            } else {
                                                // Operator-only filter like {_gt: 4.8}
                                                // Extract the field value and match against it
                                                let val = match field_name {
                                                    Some(f) => item
                                                        .as_object()
                                                        .and_then(|o| o.get(f))
                                                        .unwrap_or(&JsonValue::Null),
                                                    None => *item,
                                                };
                                                filter.matches_scalar_value(val).unwrap_or(false)
                                            }
                                        })
                                        .collect()
                                } else {
                                    items.iter().collect()
                                };

                                // Step 2: Apply order (sort array elements before limit/offset)
                                // The order field may differ from the aggregate field (e.g., order by "name", sum "rating")
                                // Supports nested paths (e.g., order: {publisher: {yearOpened: ASC}})
                                let mut ordered_items = filtered_items;
                                if let Some(ref order) = target.order {
                                    if let Some(condition) = order.conditions.first() {
                                        let fields = &condition.fields;
                                        let desc = matches!(
                                            condition.direction,
                                            crate::mapper::OrderDirection::Desc
                                        );
                                        ordered_items.sort_by(|a, b| {
                                            let resolve_value =
                                                |item: &&JsonValue| -> Option<JsonValue> {
                                                    // For scalar inline arrays, order: ASC/DESC has no field path
                                                    // (fields is either empty or contains a single empty string)
                                                    if fields.is_empty()
                                                        || (fields.len() == 1
                                                            && fields[0].is_empty())
                                                    {
                                                        return Some((*item).clone());
                                                    }
                                                    // Start with the first field
                                                    let first = &fields[0];
                                                    let mut current = item
                                                        .as_object()
                                                        .and_then(|o| o.get(first.as_str()))
                                                        .cloned()?;
                                                    // Resolve remaining nested fields
                                                    for key in &fields[1..] {
                                                        current = match current {
                                                            JsonValue::Object(ref obj) => {
                                                                obj.get(key.as_str())?.clone()
                                                            }
                                                            _ => return None,
                                                        };
                                                    }
                                                    Some(current)
                                                };
                                            let a_val = resolve_value(a);
                                            let b_val = resolve_value(b);
                                            let cmp = crate::plan::compare_json_values(
                                                a_val.as_ref(),
                                                b_val.as_ref(),
                                            );
                                            if desc {
                                                cmp.reverse()
                                            } else {
                                                cmp
                                            }
                                        });
                                    }
                                }

                                // Step 3: Apply limit/offset
                                let final_items: Vec<&JsonValue> =
                                    if let Some(ref limit) = target.limit {
                                        let offset = limit.offset as usize;
                                        let sliced = if offset < ordered_items.len() {
                                            &ordered_items[offset..]
                                        } else {
                                            &[][..]
                                        };
                                        match limit.limit {
                                            Some(l) => {
                                                sliced.iter().take(l as usize).copied().collect()
                                            }
                                            None => sliced.to_vec(),
                                        }
                                    } else {
                                        ordered_items
                                    };

                                // Step 4: Compute aggregate over final items
                                match agg_type {
                                    AggregateType::Count => {
                                        if let Some(group_by) = &target.group_by {
                                            let groups = final_items
                                                .iter()
                                                .map(|item| {
                                                    JsonValue::Array(
                                                        group_by
                                                            .fields
                                                            .iter()
                                                            .map(|field| {
                                                                item.as_object()
                                                                    .and_then(|obj| obj.get(field))
                                                                    .cloned()
                                                                    .unwrap_or(JsonValue::Null)
                                                            })
                                                            .collect(),
                                                    )
                                                    .to_string()
                                                })
                                                .collect::<std::collections::HashSet<_>>();
                                            total_count += groups.len() as i64;
                                        } else {
                                            total_count += final_items.len() as i64;
                                        }
                                    }
                                    AggregateType::Sum | AggregateType::Average => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                total_value += n;
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Min => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n < total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Max => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n > total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Scalar data: multi-field per-document aggregate
                                // e.g., _avg(HeightM: {}, Age: {}) where HeightM is a scalar
                                if let Some(n) = relation_data.as_f64() {
                                    match agg_type {
                                        AggregateType::Count => {
                                            total_count += 1;
                                        }
                                        AggregateType::Sum | AggregateType::Average => {
                                            total_value += n;
                                            total_count += 1;
                                        }
                                        AggregateType::Min => {
                                            if total_count == 0 || n < total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                        AggregateType::Max => {
                                            if total_count == 0 || n > total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Store the computed aggregate value
                    let computed_value = match agg_type {
                        AggregateType::Count => JsonValue::Number(total_count.into()),
                        AggregateType::Sum => {
                            if total_value == total_value.floor()
                                && total_value.abs() < i64::MAX as f64
                            {
                                JsonValue::Number((total_value as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(total_value)
                                        .unwrap_or_else(|| 0.into()),
                                )
                            }
                        }
                        AggregateType::Average => {
                            if total_count > 0 {
                                let avg = total_value / total_count as f64;
                                if avg == avg.floor() && avg.abs() < i64::MAX as f64 {
                                    JsonValue::Number((avg as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(avg)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                // Go DefraDB returns 0 for average of empty/null arrays
                                JsonValue::Number(0.into())
                            }
                        }
                        AggregateType::Min | AggregateType::Max => {
                            if total_count > 0 {
                                if total_value == total_value.floor()
                                    && total_value.abs() < i64::MAX as f64
                                {
                                    JsonValue::Number((total_value as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(total_value)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                JsonValue::Null
                            }
                        }
                    };

                    obj.insert(output_name.clone(), computed_value);
                }

                // Deferred cleanup: remove relation data only used for aggregation.
                // When a selection uses an alias (e.g., `books2020: book(...)`), the
                // aggregate's raw relation data ("book") must also be removed since
                // the display data is at the alias key ("books2020").
                for relation_name in &aggregate_relation_names {
                    let selected_with_same_key = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_name && s.field.output_name() == relation_name
                        } else {
                            false
                        }
                    });
                    if !selected_with_same_key {
                        obj.remove(relation_name.as_str());
                    }
                }

                // Clean up extra fields from relation data that were added for aggregate filter evaluation
                // but weren't in the original selection. For example, if the selection was
                // `published { name }` but the aggregate filter needed `rating`, we need to remove
                // `rating` from each item in `published` after aggregate computation.
                for (relation_name, allowed_fields) in &selected_relation_fields {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(relation_name) {
                        for item in items.iter_mut() {
                            if let JsonValue::Object(item_obj) = item {
                                // Remove fields that weren't in the original selection
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                        }
                    }
                }
            }
        }

        // Apply post-aggregate filtering if needed
        // When filter uses _alias to reference computed aggregates, the SelectNode can't
        // filter during plan execution since aggregate values don't exist yet.
        // Example: filter: {_alias: {publishedCount: {_gt: 0}}}
        if let Some(ref filter) = select.filter {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            // Check if filter has _alias conditions referencing aggregate names
            if let Some(alias_conditions) = filter.conditions().get("_alias") {
                if let Some(alias_obj) = alias_conditions.as_object() {
                    let needs_post_filter = alias_obj
                        .keys()
                        .any(|k| aggregate_output_names.contains(k.as_str()));

                    if needs_post_filter {
                        results.retain(|result| {
                            if let Some(obj) = result.as_object() {
                                // Evaluate each alias condition
                                for (alias_name, condition) in alias_obj {
                                    if let Some(value) = obj.get(alias_name) {
                                        // Parse and evaluate the operator conditions
                                        if let Some(cond_obj) = condition.as_object() {
                                            for (op_str, expected) in cond_obj {
                                                if let Some(op) =
                                                    crate::mapper::FilterOp::parse(op_str)
                                                {
                                                    match op {
                                                        crate::mapper::FilterOp::Eq
                                                            if value != expected =>
                                                        {
                                                            return false;
                                                        }
                                                        crate::mapper::FilterOp::Ne
                                                            if value == expected =>
                                                        {
                                                            return false;
                                                        }
                                                        crate::mapper::FilterOp::Gt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v <= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Gte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v < e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v >= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v > e {
                                                                return false;
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // Alias field not found in result, filter it out
                                        return false;
                                    }
                                }
                            }
                            true
                        });
                    }
                }
            }
        }

        // Apply post-aggregate ordering if needed
        // When order references aggregate aliases (e.g., order: {_alias: {total: DESC}}),
        // the OrderByNode can't sort during plan execution since values don't exist yet.
        if let Some(ref order_by) = select.order_by {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            let needs_post_sort = order_by.conditions.iter().any(|c| {
                c.fields.len() == 1 && aggregate_output_names.contains(c.fields[0].as_str())
            });

            if needs_post_sort {
                results.sort_by(|a, b| {
                    for condition in &order_by.conditions {
                        if condition.fields.len() != 1 {
                            continue;
                        }
                        let field = &condition.fields[0];
                        let a_val = a.as_object().and_then(|o| o.get(field));
                        let b_val = b.as_object().and_then(|o| o.get(field));
                        let a_f = a_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let b_f = b_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let ord = a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal);
                        let ord =
                            if matches!(condition.direction, crate::mapper::OrderDirection::Desc) {
                                ord.reverse()
                            } else {
                                ord
                            };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        // Clean up internal aggregate keys from output (keys like "__agg_published__count")
        // These are only used for looking up relation data when there's a collision with
        // a relation selection.
        if !aggregate_internal_keys.is_empty() {
            for result in &mut results {
                if let JsonValue::Object(ref mut obj) = result {
                    obj.retain(|k, _| !k.starts_with("__agg_"));
                }
            }
        }

        Ok(results)
    }
}

/// Extract a numeric value from a JSON item for aggregate computation.
///
/// Handles two cases:
/// - Inline array items: raw values like `JsonValue::Number(5)` — field_name is None
/// - Relation items: objects like `{"score": 5}` — field_name specifies which key
fn extract_numeric(item: &JsonValue, field_name: Option<&str>) -> Option<f64> {
    let val = match field_name {
        Some(field) => {
            // Relation aggregate: extract field from object
            item.as_object()?.get(field)?
        }
        None => {
            // Inline array aggregate: item is the value itself
            item
        }
    };
    val.as_f64().or_else(|| val.as_i64().map(|n| n as f64))
}
