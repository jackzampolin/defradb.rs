//! Schema validation methods
//!
//! Contains SdlParser validation methods:
//! - `validate_types()` - Validates parsed type definitions

use crate::error::{QueryError, Result};

use super::parser::SdlParser;

impl<'a> SdlParser<'a> {
    pub(super) fn validate_types(&self) -> Result<()> {
        let type_names: std::collections::HashSet<_> = self.type_defs.keys().cloned().collect();
        let all_type_names: std::collections::HashSet<String> = type_names
            .iter()
            .cloned()
            .chain(self.known_external_types.iter().cloned())
            .collect();

        for type_def in self.type_defs.values() {
            for field in &type_def.fields {
                // NonNull list element types are not supported for relation types (e.g., [Dogs!])
                // Scalar array types like [Float32!], [Int!], [String!] are allowed
                if field.field_type.is_list
                    && field.field_type.element_non_null
                    && all_type_names.contains(&field.field_type.base_type)
                {
                    return Err(QueryError::parse(format!(
                        "NonNull variants for type are not supported. Type: {}",
                        field.field_type.base_type
                    )));
                }

                // Default value validation
                if let Some(ref _default_val) = field.directives.default_value {
                    let base_type = &field.field_type.base_type;

                    // @default not allowed on relation fields
                    if all_type_names.contains(base_type) {
                        return Err(QueryError::parse(format!(
                            "default value is not allowed for this field type. Name: {}, Type: {}",
                            field.name, base_type
                        )));
                    }

                    // @default not allowed on list fields
                    if field.field_type.is_list {
                        return Err(QueryError::parse(format!(
                            "default value is not allowed for this field type. Name: {}, Type: List",
                            field.name
                        )));
                    }
                }
            }
        }

        // Validate one-to-one cross-type relation @primary constraints.
        // Self-referencing relations (User→User) are excluded as they handle
        // primary/secondary through field-level @primary directives.
        // Relations with explicit @relation(name:) on both sides using DIFFERENT
        // names are separate relations and skip this check.
        {
            let mut checked_pairs = std::collections::HashSet::new();

            for type_def in self.type_defs.values() {
                for field in &type_def.fields {
                    let target = &field.field_type.base_type;

                    // Skip non-relations, array relations, and self-references
                    if !all_type_names.contains(target)
                        || field.field_type.is_list
                        || target == &type_def.name
                    {
                        continue;
                    }

                    // Check if counterpart type has a single-object field pointing back
                    // that shares the same relation (either no explicit name, or same name)
                    let counterpart_field = self.type_defs.get(target).and_then(|target_def| {
                        target_def.fields.iter().find(|f| {
                            if f.field_type.base_type != type_def.name || f.field_type.is_list {
                                return false;
                            }
                            // Only match fields as counterparts of the same relation
                            // when their explicit relation names align
                            match (&field.directives.relation_name, &f.directives.relation_name) {
                                (Some(a), Some(b)) => a == b,
                                (None, None) => true,
                                _ => false, // One named, one not → separate relations
                            }
                        })
                    });

                    let Some(counter_field) = counterpart_field else {
                        continue; // Not a one-to-one relation
                    };

                    // Avoid checking the same pair twice
                    let pair_key = if type_def.name < *target {
                        (type_def.name.clone(), target.clone())
                    } else {
                        (target.clone(), type_def.name.clone())
                    };
                    if !checked_pairs.insert(pair_key) {
                        continue;
                    }

                    let this_has_primary = field.directives.is_primary;
                    let counterpart_has_primary = counter_field.directives.is_primary;

                    // Both sides have @primary → error
                    if this_has_primary && counterpart_has_primary {
                        return Err(QueryError::parse(
                            "relation can only have a single field set as primary",
                        ));
                    }

                    // Neither side has @primary → error
                    if !this_has_primary && !counterpart_has_primary {
                        return Err(QueryError::parse(format!(
                            "relation missing field. Object type {}, Field {}",
                            type_def.name, field.name
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
