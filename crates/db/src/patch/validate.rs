use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Validate the patched schema JSON and return a CollectionVersion.
    ///
    /// Deserializes the modified JSON, validates VersionID, checks field
    /// mutations/kinds/names, auto-generates relation ID fields, and
    /// validates self-referential relation completeness.
    pub(crate) fn validate_patched_schema(
        &self,
        schema_json: serde_json::Value,
        old_schema: &CollectionVersion,
        actual_name: &str,
    ) -> Result<CollectionVersion> {
        // Field movement and duplicate checks are handled by the definition validators
        // (validate_field_not_moved, validate_field_not_duplicated) which run post-deserialization.
        // This matches Go's approach of collecting ALL validation errors at once.

        // Deserialize back to CollectionVersion
        let mut new_schema: CollectionVersion = serde_json::from_value(schema_json)
            .map_err(|e| Error::InvalidPatch(format!("invalid resulting schema: {}", e)))?;

        // Go compatibility: check for removed/empty required fields after deserialization.
        // Go's JSON unmarshaling uses zero values for missing fields; our serde defaults
        // replicate this. Now check for invalid empty values that indicate patch corruption.
        if new_schema.version_id.is_empty() {
            return Err(Error::InvalidPatch(
                "invalid cid: cid too short. VersionID: ".to_string(),
            ));
        }

        // Go compatibility: validate field Kind and Name after patching.
        // Existing fields (matched by FieldID) that were modified are "mutations"
        // which is not supported. New fields with Kind:0 or empty Name are separate errors.
        {
            let old_field_ids: std::collections::HashSet<&str> =
                old_schema.fields.iter().map(|f| f.id.as_str()).collect();
            let old_field_map: std::collections::HashMap<&str, &schema::FieldDescription> =
                old_schema
                    .fields
                    .iter()
                    .map(|f| (f.id.as_str(), f))
                    .collect();
            let mut field_errors = Vec::new();

            for field in &new_schema.fields {
                let is_old_field =
                    !field.id.is_empty() && old_field_ids.contains(field.id.as_str());

                if is_old_field {
                    // Check if an existing field was mutated (Kind changed, Name removed, etc.)
                    if let Some(old_field) = old_field_map.get(field.id.as_str()) {
                        let kind_changed = field.kind != old_field.kind;
                        let name_empty = field.name.is_empty();
                        if kind_changed || name_empty {
                            field_errors.push(
                                "mutating an existing field is not supported. ProposedName: "
                                    .to_string(),
                            );
                        }
                    }
                } else {
                    // Check if this is a relational ID field (_<name>ID pattern)
                    // A field is a relational ID if:
                    // 1. Its name matches _<base>ID where <base> is another field name
                    // 2. That base field has a relation-like Kind (Named, Relation, or SelfRef)
                    // Note: we use is_relation() which checks the Kind variant directly
                    let is_relational_id = field
                        .name
                        .strip_prefix('_')
                        .and_then(|s| s.strip_suffix("ID"))
                        .map(|base_name| {
                            new_schema.fields.iter().any(|f| {
                                f.name == base_name
                                    && matches!(
                                        f.kind,
                                        schema::FieldKind::Named { .. }
                                            | schema::FieldKind::Relation { .. }
                                            | schema::FieldKind::SelfRef { .. }
                                    )
                            })
                        })
                        .unwrap_or(false);

                    if is_relational_id {
                        // Relational ID fields must have Kind = DocID (1)
                        if !matches!(
                            field.kind,
                            schema::FieldKind::Scalar(schema::ScalarKind::DocID)
                        ) {
                            let kind_display = match &field.kind {
                                schema::FieldKind::Scalar(k) => match k {
                                    schema::ScalarKind::None => "0".to_string(),
                                    schema::ScalarKind::DocID => "ID".to_string(),
                                    schema::ScalarKind::Bool => "Boolean".to_string(),
                                    schema::ScalarKind::Int => "Integer".to_string(),
                                    schema::ScalarKind::Float64 => "Float".to_string(),
                                    schema::ScalarKind::Float32 => "Float32".to_string(),
                                    schema::ScalarKind::DateTime => "DateTime".to_string(),
                                    schema::ScalarKind::String => "String".to_string(),
                                    schema::ScalarKind::Blob => "Blob".to_string(),
                                    schema::ScalarKind::Json => "JSON".to_string(),
                                },
                                other => format!("{:?}", other),
                            };
                            field_errors.push(format!(
                                "relational id field of invalid kind. Field: {}, Expected: ID, Actual: {}",
                                field.name, kind_display
                            ));
                        }
                    } else {
                        // Standard new field validations
                        if matches!(
                            field.kind,
                            schema::FieldKind::Scalar(schema::ScalarKind::None)
                        ) && field.name != "_docID"
                        {
                            field_errors.push(format!(
                                "no type found for given name. Type: {}",
                                schema::ScalarKind::None as u8
                            ));
                        }
                        if field.name.is_empty() {
                            field_errors.push(
                                "Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but \"\" does not."
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            if !field_errors.is_empty() {
                return Err(Error::InvalidPatch(field_errors.join("\n")));
            }
        }

        // Go compatibility: auto-generate _fieldID for foreign object fields added via patch.
        // This matches Go's collection_define.go behavior for fields with Kind.IsObject() && !Kind.IsArray().
        {
            let max_field_id: u64 = new_schema
                .fields
                .iter()
                .filter_map(|f| f.id.parse::<u64>().ok())
                .max()
                .unwrap_or(0);
            let mut next_id = max_field_id + 1;
            new_schema
                .add_relation_id_fields(|| {
                    let id = next_id.to_string();
                    next_id += 1;
                    id
                })
                .map_err(|e| {
                    Error::InvalidPatch(format!("failed to add relation id fields: {}", e))
                })?;
        }

        // Go compatibility: validate self-referential relation field completeness.
        // For self-references (where field.Kind references the same collection), the schema
        // must have another field with the same RelationName (the other side of the relation).
        // This only applies to self-references; cross-collection relations are validated
        // at a later stage after all patches have been applied.
        {
            let mut rel_errors = Vec::new();
            for field in &new_schema.fields {
                if !field.kind.is_relation() {
                    continue;
                }
                let relation_name = match &field.relation_name {
                    Some(rn) => rn.clone(),
                    None => continue,
                };

                // Get the referenced collection name from the Kind
                let ref_name = match &field.kind {
                    schema::FieldKind::Named { name, .. } => name.clone(),
                    schema::FieldKind::Relation { collection_id, .. } => collection_id.clone(),
                    schema::FieldKind::SelfRef { .. } => new_schema.name.clone(),
                    _ => continue,
                };

                // Only check self-references (where the Kind references this same collection)
                let is_self_ref = ref_name == new_schema.name || ref_name == actual_name;
                if !is_self_ref {
                    continue;
                }

                // Check if there's another field with the same relation name
                let has_counterpart = new_schema.fields.iter().any(|f| {
                    f.name != field.name
                        && f.relation_name.as_deref() == Some(relation_name.as_str())
                });

                if !has_counterpart {
                    rel_errors.push(format!(
                        "relation missing field. Object: {}, RelationName: {}",
                        ref_name, relation_name
                    ));
                }
            }

            if !rel_errors.is_empty() {
                return Err(Error::InvalidPatch(rel_errors.join("\n")));
            }
        }

        Ok(new_schema)
    }
}
