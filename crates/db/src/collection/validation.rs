use super::*;

impl Collection {
    /// Validate a document against this collection's schema.
    ///
    /// Returns an error if the document contains fields with incorrect types.
    /// Unknown fields (not in schema) are allowed for flexibility.
    pub(crate) fn validate_document(&self, doc: &Document) -> Result<()> {
        for field_def in &self.def.fields {
            // Skip _docID field - it's handled separately
            if field_def.name == "_docID" {
                continue;
            }

            // Get the value for this field (if present)
            if let Some(value) = doc.get(&field_def.name) {
                // Validate the value type matches the schema
                if !is_value_compatible_with_kind(value, &field_def.kind) {
                    return Err(Error::InvalidDocument(format!(
                        "Field '{}' has incompatible type: expected {:?}, got {:?}",
                        field_def.name, field_def.kind, value
                    )));
                }
            }
            // Missing fields are allowed (nullable by default in DefraDB)
        }
        Ok(())
    }

    pub(crate) fn validate_immutable_fields_unchanged(
        &self,
        old_doc: &Document,
        new_doc: &Document,
    ) -> Result<()> {
        for field_def in self.def.fields.iter().filter(|field| field.immutable) {
            let old_value = old_doc.get(&field_def.name);
            let new_value = new_doc.get(&field_def.name);
            if old_value != new_value {
                return Err(Error::InvalidDocument(format!(
                    "immutable field '{}' cannot be changed",
                    field_def.name
                )));
            }
        }
        Ok(())
    }
}

/// Check if a NormalValue is compatible with a FieldKind.
fn is_value_compatible_with_kind(value: &NormalValue, kind: &FieldKind) -> bool {
    // Null is compatible with all nillable types (which is everything in DefraDB)
    if value.is_nil() {
        return true;
    }

    match kind {
        FieldKind::Scalar(scalar) => is_value_compatible_with_scalar(value, *scalar),
        FieldKind::ScalarArray(array) => is_value_compatible_with_array(value, *array),
        // Relations are stored as document IDs (strings) or nested documents
        FieldKind::Relation { is_array, .. }
        | FieldKind::SelfRef { is_array, .. }
        | FieldKind::Named { is_array, .. } => {
            if *is_array {
                matches!(
                    value,
                    NormalValue::StringArray(_) | NormalValue::DocumentArray(_)
                )
            } else {
                matches!(value, NormalValue::String(_) | NormalValue::Document(_))
            }
        }
        _ => false,
    }
}

/// Check if a NormalValue is compatible with a ScalarKind.
fn is_value_compatible_with_scalar(value: &NormalValue, scalar: ScalarKind) -> bool {
    match scalar {
        ScalarKind::None => true,
        ScalarKind::DocID => matches!(value, NormalValue::String(_)),
        ScalarKind::Bool => matches!(value, NormalValue::Bool(_) | NormalValue::NillableBool(_)),
        ScalarKind::Int => matches!(value, NormalValue::Int(_) | NormalValue::NillableInt(_)),
        ScalarKind::Float64 => {
            // Accept Int values for Float64 fields (common in JSON where 5 and 5.0 are equivalent)
            matches!(
                value,
                NormalValue::Float64(_)
                    | NormalValue::NillableFloat64(_)
                    | NormalValue::Int(_)
                    | NormalValue::NillableInt(_)
            )
        }
        ScalarKind::Float32 => {
            // Accept Int and Float64 values for Float32 fields (JSON only has one float type)
            matches!(
                value,
                NormalValue::Float32(_)
                    | NormalValue::NillableFloat32(_)
                    | NormalValue::Float64(_)
                    | NormalValue::NillableFloat64(_)
                    | NormalValue::Int(_)
                    | NormalValue::NillableInt(_)
            )
        }
        ScalarKind::DateTime => match value {
            NormalValue::Time(_) | NormalValue::NillableTime(_) => true,
            // Document storage is schema-blind for DateTime: a `Time` round-trips
            // through CBOR as an untagged text string and reads back as `String`
            // (see document::encoding::coerce_stored_value_for_kind, which the
            // index path uses for the same reason). So updating ANY field on a
            // document that already holds a DateTime re-validates the stored value
            // as a String. Accept a String iff it parses as RFC3339 — a stored
            // `Time` always does, while genuinely-wrong strings still fail.
            NormalValue::String(s) => chrono::DateTime::parse_from_rfc3339(s).is_ok(),
            NormalValue::NillableString(Some(s)) => chrono::DateTime::parse_from_rfc3339(s).is_ok(),
            _ => false,
        },
        ScalarKind::String => {
            matches!(
                value,
                NormalValue::String(_) | NormalValue::NillableString(_)
            )
        }
        ScalarKind::Blob => {
            // Accept String values for Blob fields (hex-encoded strings from JSON)
            matches!(
                value,
                NormalValue::Bytes(_) | NormalValue::NillableBytes(_) | NormalValue::String(_)
            )
        }
        // Accept both NormalValue::Json and NormalValue::String for JSON fields
        // String values are used for @default JSON values (stored as serialized strings)
        ScalarKind::Json => matches!(value, NormalValue::Json(_) | NormalValue::String(_)),
        _ => false,
    }
}

/// Check if a NormalValue is compatible with a ScalarArrayKind.
fn is_value_compatible_with_array(value: &NormalValue, array: ScalarArrayKind) -> bool {
    // Accept empty arrays of any type (JSON can't infer type from empty array)
    if is_empty_array(value) {
        return true;
    }

    match array {
        ScalarArrayKind::BoolArray => matches!(value, NormalValue::BoolArray(_)),
        ScalarArrayKind::IntArray => matches!(value, NormalValue::IntArray(_)),
        ScalarArrayKind::Float64Array => {
            // Accept Int and Float32 arrays for Float64 fields (JSON might parse as ints,
            // embedding providers may return f32 vectors)
            matches!(
                value,
                NormalValue::Float64Array(_)
                    | NormalValue::Float32Array(_)
                    | NormalValue::IntArray(_)
            )
        }
        ScalarArrayKind::Float32Array => {
            // Accept Int and Float64 arrays for Float32 fields
            matches!(
                value,
                NormalValue::Float32Array(_)
                    | NormalValue::Float64Array(_)
                    | NormalValue::IntArray(_)
            )
        }
        ScalarArrayKind::StringArray => matches!(value, NormalValue::StringArray(_)),
        // Nillable arrays: also accept the non-nillable version
        ScalarArrayKind::NillableBoolArray => {
            matches!(
                value,
                NormalValue::NillableBoolArray(_)
                    | NormalValue::NillableBoolElementArray(_)
                    | NormalValue::BoolArray(_)
            )
        }
        ScalarArrayKind::NillableIntArray => {
            matches!(
                value,
                NormalValue::NillableIntArray(_)
                    | NormalValue::NillableIntElementArray(_)
                    | NormalValue::IntArray(_)
            )
        }
        ScalarArrayKind::NillableFloat64Array => {
            matches!(
                value,
                NormalValue::NillableFloat64Array(_)
                    | NormalValue::NillableFloat64ElementArray(_)
                    | NormalValue::Float64Array(_)
                    | NormalValue::IntArray(_)
            )
        }
        ScalarArrayKind::NillableFloat32Array => {
            matches!(
                value,
                NormalValue::NillableFloat32Array(_)
                    | NormalValue::NillableFloat32ElementArray(_)
                    | NormalValue::Float32Array(_)
                    | NormalValue::Float64Array(_)
                    | NormalValue::IntArray(_)
            )
        }
        ScalarArrayKind::NillableStringArray => {
            matches!(
                value,
                NormalValue::NillableStringArray(_)
                    | NormalValue::NillableStringElementArray(_)
                    | NormalValue::StringArray(_)
            )
        }
        _ => false,
    }
}

/// Check if a NormalValue is an empty array of any type.
fn is_empty_array(value: &NormalValue) -> bool {
    match value {
        NormalValue::BoolArray(arr) => arr.is_empty(),
        NormalValue::IntArray(arr) => arr.is_empty(),
        NormalValue::Float32Array(arr) => arr.is_empty(),
        NormalValue::Float64Array(arr) => arr.is_empty(),
        NormalValue::StringArray(arr) => arr.is_empty(),
        // NillableXxxArray wraps Option<Vec<_>>
        NormalValue::NillableBoolArray(opt) => opt.as_ref().is_none_or(|v| v.is_empty()),
        NormalValue::NillableIntArray(opt) => opt.as_ref().is_none_or(|v| v.is_empty()),
        NormalValue::NillableFloat32Array(opt) => opt.as_ref().is_none_or(|v| v.is_empty()),
        NormalValue::NillableFloat64Array(opt) => opt.as_ref().is_none_or(|v| v.is_empty()),
        NormalValue::NillableStringArray(opt) => opt.as_ref().is_none_or(|v| v.is_empty()),
        // NillableXxxElementArray wraps Vec<Option<_>>
        NormalValue::NillableBoolElementArray(arr) => arr.is_empty(),
        NormalValue::NillableIntElementArray(arr) => arr.is_empty(),
        NormalValue::NillableFloat32ElementArray(arr) => arr.is_empty(),
        NormalValue::NillableFloat64ElementArray(arr) => arr.is_empty(),
        NormalValue::NillableStringElementArray(arr) => arr.is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    fn filtered_collection() -> Collection {
        Collection::new(CollectionVersion::new(
            "AgentDoc",
            "agent_doc_version",
            "agent_doc_collection",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "agent_did", FieldKind::string()).as_immutable(),
                FieldDescription::new("3", "body", FieldKind::string()),
            ],
        ))
    }

    #[test]
    fn immutable_field_validation_allows_other_field_changes() {
        let collection = filtered_collection();
        let old_doc =
            Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"before"}"#).unwrap();
        let new_doc =
            Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"after"}"#).unwrap();

        collection
            .validate_immutable_fields_unchanged(&old_doc, &new_doc)
            .unwrap();
    }

    #[test]
    fn immutable_field_validation_rejects_key_changes() {
        let collection = filtered_collection();
        let old_doc =
            Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"before"}"#).unwrap();
        let new_doc =
            Document::from_json_str(r#"{"agent_did":"did:key:other","body":"before"}"#).unwrap();

        let result = collection.validate_immutable_fields_unchanged(&old_doc, &new_doc);
        assert!(
            matches!(result, Err(Error::InvalidDocument(message)) if message.contains("agent_did"))
        );
    }
}
