//! Index value extraction and Cartesian product logic.

use crate::error::{Error, Result};
use document::NormalValue;
use schema::{CollectionVersion, IndexDescription};

use super::IndexManager;

impl IndexManager {
    /// Extract field values from a document for indexing.
    ///
    /// # Multi-Value Indexing (Arrays)
    ///
    /// When a field contains an array, multiple index entries are created - one per
    /// array element. For composite indexes with multiple array fields, the Cartesian
    /// product of all array elements is generated.
    ///
    /// Example: For document `{tags: ["a", "b"], categories: ["x", "y"]}` with a
    /// composite index on `(tags, categories)`, four index entries are created:
    /// `("a", "x")`, `("a", "y")`, `("b", "x")`, `("b", "y")`
    ///
    /// # Null Handling
    ///
    /// If a document is missing a field that is part of an index, the value is
    /// indexed as `NormalValue::Null`. This is intentional for nullable fields
    /// and allows documents with missing optional fields to be indexed.
    ///
    /// For unique indexes, multiple documents with NULL values for the same
    /// indexed field will all be indexed (NULL is not considered equal to NULL
    /// for uniqueness purposes).
    pub(crate) fn extract_index_values(
        &self,
        doc: &document::Document,
        index_desc: &IndexDescription,
        schema: &CollectionVersion,
    ) -> Result<Vec<Vec<NormalValue>>> {
        let schema_fields: std::collections::HashSet<&str> =
            schema.fields.iter().map(|f| f.name.as_str()).collect();

        let mut field_value_sets: Vec<Vec<NormalValue>> =
            Vec::with_capacity(index_desc.fields.len());

        for field in &index_desc.fields {
            if !field.name.starts_with('_') && !schema_fields.contains(field.name.as_str()) {
                return Err(Error::Other(format!(
                    "index '{}' references field '{}' which does not exist in schema",
                    index_desc.name, field.name
                )));
            }

            let value = doc.get(&field.name).cloned().unwrap_or(NormalValue::Null);

            // Repair the schema-blind CBOR round-trip: a DateTime field loaded
            // from storage comes back as a String (see document::encoding_cbor),
            // which the index encoder would place in a disjoint byte range from
            // live-written Time entries — silently hiding the row from DateTime
            // cursor/range queries. Coerce to the declared kind before encoding.
            let value = match schema.fields.iter().find(|f| f.name == field.name) {
                Some(schema::FieldDescription {
                    kind: schema::FieldKind::Scalar(kind),
                    ..
                }) => document::encoding::coerce_stored_value_for_kind(value, kind),
                _ => value,
            };

            // A vector is one value, not a set of them. Expanding it would
            // index each component as its own entry, which is both wrong and
            // enormous: a 768-dimension embedding would become 768 entries.
            let expanded = if index_desc.is_vector() {
                vec![value]
            } else {
                Self::expand_value_for_indexing(value)
            };
            field_value_sets.push(expanded);
        }

        Ok(Self::cartesian_product(field_value_sets))
    }

    /// Expand a value for multi-value indexing.
    ///
    /// Arrays are expanded into their elements. Empty arrays result in a single
    /// NULL value to ensure the document is still indexed.
    pub(super) fn expand_value_for_indexing(value: NormalValue) -> Vec<NormalValue> {
        macro_rules! expand_array {
            ($arr:expr, $variant:ident) => {
                if $arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    Self::deduplicate_array_values($arr.iter().cloned())
                        .into_iter()
                        .map(NormalValue::$variant)
                        .collect()
                }
            };
        }

        macro_rules! expand_nillable_array {
            ($opt:expr, $variant:ident) => {
                match $opt {
                    Some(arr) => {
                        if arr.is_empty() {
                            vec![NormalValue::Null]
                        } else {
                            Self::deduplicate_array_values(arr.iter().cloned())
                                .into_iter()
                                .map(NormalValue::$variant)
                                .collect()
                        }
                    }
                    None => vec![NormalValue::Null],
                }
            };
        }

        macro_rules! expand_nillable_element_array {
            ($arr:expr, $variant:ident) => {
                if $arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    Self::deduplicate_array_values($arr.iter().cloned())
                        .into_iter()
                        .map(|v| match v {
                            Some(val) => NormalValue::$variant(val),
                            None => NormalValue::Null,
                        })
                        .collect()
                }
            };
        }

        match value {
            NormalValue::Json(_) => {
                let leaves = value.json_leaves();
                if leaves.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    leaves
                }
            }

            NormalValue::Null
            | NormalValue::Bool(_)
            | NormalValue::Int(_)
            | NormalValue::Float64(_)
            | NormalValue::Float32(_)
            | NormalValue::String(_)
            | NormalValue::Bytes(_)
            | NormalValue::Time(_)
            | NormalValue::Document(_)
            | NormalValue::JsonLeaf(_)
            | NormalValue::NillableBool(_)
            | NormalValue::NillableInt(_)
            | NormalValue::NillableFloat64(_)
            | NormalValue::NillableFloat32(_)
            | NormalValue::NillableString(_)
            | NormalValue::NillableBytes(_)
            | NormalValue::NillableTime(_)
            | NormalValue::NillableDocument(_) => vec![value],

            NormalValue::BoolArray(ref arr) => expand_array!(arr, Bool),
            NormalValue::IntArray(ref arr) => expand_array!(arr, Int),
            NormalValue::Float64Array(ref arr) => expand_array!(arr, Float64),
            NormalValue::Float32Array(ref arr) => expand_array!(arr, Float32),
            NormalValue::StringArray(ref arr) => expand_array!(arr, String),
            NormalValue::BytesArray(ref arr) => expand_array!(arr, Bytes),
            NormalValue::TimeArray(ref arr) => expand_array!(arr, Time),
            NormalValue::DocumentArray(ref arr) => {
                if arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    arr.iter()
                        .map(|v| NormalValue::Document(Box::new(v.clone())))
                        .collect()
                }
            }
            // JSON array positions are part of their index paths, so repeated values stay distinct.
            NormalValue::JsonArray(ref arr) => {
                if arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    arr.iter().cloned().map(NormalValue::Json).collect()
                }
            }

            NormalValue::NillableBoolArray(ref opt) => expand_nillable_array!(opt, Bool),
            NormalValue::NillableIntArray(ref opt) => expand_nillable_array!(opt, Int),
            NormalValue::NillableFloat64Array(ref opt) => expand_nillable_array!(opt, Float64),
            NormalValue::NillableFloat32Array(ref opt) => expand_nillable_array!(opt, Float32),
            NormalValue::NillableStringArray(ref opt) => expand_nillable_array!(opt, String),
            NormalValue::NillableBytesArray(ref opt) => expand_nillable_array!(opt, Bytes),
            NormalValue::NillableTimeArray(ref opt) => expand_nillable_array!(opt, Time),
            NormalValue::NillableDocumentArray(ref opt) => match opt {
                Some(arr) => {
                    if arr.is_empty() {
                        vec![NormalValue::Null]
                    } else {
                        arr.iter()
                            .map(|v| NormalValue::Document(Box::new(v.clone())))
                            .collect()
                    }
                }
                None => vec![NormalValue::Null],
            },

            NormalValue::NillableBoolElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Bool)
            }
            NormalValue::NillableIntElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Int)
            }
            NormalValue::NillableFloat64ElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Float64)
            }
            NormalValue::NillableFloat32ElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Float32)
            }
            NormalValue::NillableStringElementArray(ref arr) => {
                expand_nillable_element_array!(arr, String)
            }
            NormalValue::NillableBytesElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Bytes)
            }
            NormalValue::NillableTimeElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Time)
            }
            NormalValue::NillableDocumentElementArray(ref arr) => {
                if arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    arr.iter()
                        .map(|v| match v {
                            Some(doc) => NormalValue::Document(Box::new(doc.clone())),
                            None => NormalValue::Null,
                        })
                        .collect()
                }
            }
            _ => unreachable!(),
        }
    }

    fn deduplicate_array_values<T: PartialEq>(values: impl IntoIterator<Item = T>) -> Vec<T> {
        let mut unique = Vec::new();
        for value in values {
            if !unique.contains(&value) {
                unique.push(value);
            }
        }
        unique
    }

    /// Compute Cartesian product of field value sets.
    ///
    /// Given `[[a, b], [x, y]]`, produces `[[a, x], [a, y], [b, x], [b, y]]`.
    pub(super) fn cartesian_product(sets: Vec<Vec<NormalValue>>) -> Vec<Vec<NormalValue>> {
        if sets.is_empty() {
            return vec![vec![]];
        }

        let mut result: Vec<Vec<NormalValue>> = vec![vec![]];

        for set in sets {
            let mut new_result = Vec::with_capacity(result.len() * set.len());
            for combo in &result {
                for val in &set {
                    let mut new_combo = combo.clone();
                    new_combo.push(val.clone());
                    new_result.push(new_combo);
                }
            }
            result = new_result;
        }

        result
    }
}

#[cfg(test)]
mod coercion_tests {
    use super::*;
    use document::Document;
    use schema::{
        CollectionVersion, FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription,
    };

    fn datetime_index() -> IndexDescription {
        IndexDescription {
            name: "idx_created_at".to_string(),
            id: 1,
            unique: false,
            kind: None,
            auto_generated: false,
            fields: vec![IndexedFieldDescription {
                name: "created_at".to_string(),
                descending: true,
            }],
        }
    }

    fn collection() -> CollectionVersion {
        let mut c = CollectionVersion::new(
            "CodingSession",
            "v1",
            "coll_cs",
            vec![FieldDescription::new(
                "1",
                "created_at",
                FieldKind::datetime(),
            )],
        );
        c.indexes = vec![datetime_index()];
        c
    }

    /// #72 regression: a DateTime field loaded from CBOR storage comes back as a
    /// String. The index builder must coerce it to Time so its entry lands in
    /// the same byte range as live-written rows — otherwise it is silently
    /// excluded from `order:[{created_at: DESC}]` cursor queries. A plain
    /// `maintenance reindex` (pass-through branch) feeds exactly these docs.
    #[test]
    fn datetime_field_stored_as_string_is_indexed_as_time() {
        let schema = collection();
        let idx = datetime_index();
        let mgr = IndexManager::from_collection(1, &schema).unwrap();

        let mut doc = Document::new();
        doc.set(
            "created_at",
            NormalValue::String("2026-05-29T13:06:28Z".to_string()),
        );

        let rows = mgr.extract_index_values(&doc, &idx, &schema).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            matches!(rows[0][0], NormalValue::Time(_)),
            "String DateTime from storage must index as Time, got {:?}",
            rows[0][0]
        );
    }

    /// A genuine String field holding a date-like value must NOT be coerced.
    #[test]
    fn string_field_with_date_like_value_is_left_as_string() {
        let mut schema = CollectionVersion::new(
            "CodingSession",
            "v1",
            "coll_cs",
            vec![FieldDescription::new("1", "label", FieldKind::string())],
        );
        let idx = IndexDescription {
            name: "idx_label".to_string(),
            id: 2,
            unique: false,
            kind: None,
            auto_generated: false,
            fields: vec![IndexedFieldDescription {
                name: "label".to_string(),
                descending: false,
            }],
        };
        schema.indexes = vec![idx.clone()];
        let mgr = IndexManager::from_collection(1, &schema).unwrap();

        let mut doc = Document::new();
        doc.set(
            "label",
            NormalValue::String("2026-05-29T13:06:28Z".to_string()),
        );

        let rows = mgr.extract_index_values(&doc, &idx, &schema).unwrap();
        assert!(
            matches!(rows[0][0], NormalValue::String(_)),
            "String field must stay String, got {:?}",
            rows[0][0]
        );
    }
}
