//! Field kind resolution for SDL builder.

use query_types::error::{QueryError, Result};
use schema::FieldKind;

use super::helpers::graphql_to_scalar_kind;
use super::parser::{ParsedType, SdlParser};

impl<'a> SdlParser<'a> {
    pub(super) fn resolve_field_kind(
        &self,
        parsed_type: &ParsedType,
        field_name: &str,
        type_names: &std::collections::HashSet<String>,
        current_type: &str,
        collection_set: &std::collections::HashMap<String, (i32, usize)>,
        known_collection_ids: &std::collections::HashMap<String, String>,
    ) -> Result<FieldKind> {
        let base = &parsed_type.base_type;

        // Check if it's a scalar type
        if let Some(scalar_kind) = graphql_to_scalar_kind(base) {
            if parsed_type.is_list {
                let array_kind = if parsed_type.element_non_null {
                    scalar_kind.to_array_kind()
                } else {
                    scalar_kind.to_nillable_array_kind()
                };

                return array_kind.map(FieldKind::ScalarArray).ok_or_else(|| {
                    QueryError::parse(format!("scalar type {} cannot be used in arrays", base))
                });
            }
            if parsed_type.is_non_null {
                return scalar_kind
                    .to_non_nillable()
                    .map(FieldKind::Scalar)
                    .ok_or_else(|| {
                        QueryError::parse(format!(
                            "NonNull variant for type {} is not supported",
                            base
                        ))
                    });
            }
            return Ok(FieldKind::Scalar(scalar_kind));
        }

        // Check if it's a self-reference (same type or "Self" keyword)
        if base == current_type || base == "Self" {
            // Single-collection self-ref sets omit RelativeID, but a self-reference inside
            // a multi-collection set must use this collection's relative ID so nested
            // self-relations resolve against the correct collection-set member.
            if let Some(&(relative_id, group_idx)) = collection_set.get(current_type) {
                let is_multi_collection_set =
                    collection_set.iter().any(|(name, &(_, other_group_idx))| {
                        name != current_type && other_group_idx == group_idx
                    });
                if is_multi_collection_set {
                    return Ok(FieldKind::self_ref(
                        relative_id.to_string(),
                        parsed_type.is_list,
                    ));
                }
            }
            return Ok(FieldKind::self_ref("", parsed_type.is_list));
        }

        // Check if it references another type in the schema
        if type_names.contains(base) {
            // This is a relation to another type
            // If both the current type and target type are in the SAME collection set
            // (circular relations), use SelfRef with the target's relative index
            if let (Some(&(target_idx, target_group)), Some(&(_, current_group))) =
                (collection_set.get(base), collection_set.get(current_type))
            {
                if target_group == current_group {
                    return Ok(FieldKind::self_ref(
                        target_idx.to_string(),
                        parsed_type.is_list,
                    ));
                }
            }

            // If target type was already processed (alphabetically earlier),
            // use Relation with the known CollectionID (matches Go behavior)
            if let Some(collection_id) = known_collection_ids.get(base) {
                return Ok(FieldKind::relation(
                    collection_id.clone(),
                    parsed_type.is_list,
                ));
            }

            // For non-circular relations where target not yet processed, use Named
            return Ok(FieldKind::named(base, parsed_type.is_list));
        }

        // Unknown type - error for Go compatibility
        Err(QueryError::parse(format!(
            "no type found for given name. Field: {}, Kind: {}",
            field_name, base
        )))
    }
}
