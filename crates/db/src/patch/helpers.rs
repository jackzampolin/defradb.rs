use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Validate a Kind value in a patch field addition.
    /// Returns error if the Kind is an unsupported numeric value or unknown string.
    pub(crate) fn validate_patch_field_kind(
        kind_val: &serde_json::Value,
        field_name: &str,
        known_collections: &[String],
    ) -> Result<()> {
        match kind_val {
            serde_json::Value::Number(n) => {
                let kind_num = n.as_u64().unwrap_or(0) as u8;
                // Valid numeric kinds: 1-14, 18-22 (0 is None, only for internal _docID)
                let valid = matches!(kind_num, 1..=14 | 18..=22);
                if !valid {
                    return Err(Error::InvalidPatch(format!(
                        "no type found for given name. Type: {}",
                        kind_num
                    )));
                }
                Ok(())
            }
            serde_json::Value::String(s) => {
                // Known string kinds
                let known = matches!(
                    s.as_str(),
                    "ID" | "Boolean"
                        | "Int"
                        | "DateTime"
                        | "Float"
                        | "Float64"
                        | "Float32"
                        | "String"
                        | "Blob"
                        | "JSON"
                        | "[Boolean]"
                        | "[Boolean!]"
                        | "[Int]"
                        | "[Int!]"
                        | "[Float]"
                        | "[Float64]"
                        | "[Float!]"
                        | "[Float64!]"
                        | "[Float32]"
                        | "[Float32!]"
                        | "[String]"
                        | "[String!]"
                        | "Self"
                        | "[Self]"
                );
                if !known {
                    // Could be a collection name reference (e.g., "Users", "[Users]").
                    let ref_name = s
                        .strip_prefix('[')
                        .and_then(|s| s.strip_suffix(']'))
                        .unwrap_or(s.as_str());
                    if !known_collections.iter().any(|c| c == ref_name) {
                        return Err(Error::InvalidPatch(format!(
                            "no type found for given name. Field: {}, Kind: {}",
                            field_name, s
                        )));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Strip collection name or version ID prefix from a path.
    ///
    /// Handles both the collection_name prefix (e.g., "/Users/") and the
    /// actual_name prefix (when looked up by version ID, the passed-in name
    /// differs from the real collection name).
    pub(crate) fn strip_collection_prefix(path: &str, prefixes: &[String]) -> String {
        // Go DefraDB accepts paths with or without leading '/'.
        // Try each prefix (collection name, actual name, version ID) in order.
        for prefix in prefixes {
            let no_slash_prefix = prefix.trim_start_matches('/');

            if let Some(rest) = path.strip_prefix(prefix.as_str()) {
                return format!("/{}", rest);
            }
            if let Some(rest) = path.strip_prefix(no_slash_prefix) {
                return format!("/{}", rest);
            }
            // Exact match without trailing slash (collection-level operations).
            // E.g., path="/Users" with prefix="/Users/" → "/"
            let exact = prefix.trim_end_matches('/');
            let exact_no_slash = exact.trim_start_matches('/');
            if path == exact || path == exact_no_slash {
                return "/".to_string();
            }
        }
        path.to_string()
    }

    /// Handle patches targeting a collection that doesn't exist by name or version ID.
    ///
    /// This handles several cases:
    /// 1. Schema field names (EncryptedIndexes, Indexes, etc.) - produce JSON patch errors
    ///    because these fields don't exist in Go's JSON representation
    /// 2. Collection-level copy where the "path" targets a new collection name
    ///    (e.g., copy from /Users to /Book) → returns "adding collections not supported"
    /// 3. Collection-level move to a new name (no-op in Go) → finds source via "from"
    ///    and returns the original schema unchanged
    pub(crate) async fn handle_unknown_collection_patch(
        &self,
        collection_name: &str,
        patch_ops: &serde_json::Value,
    ) -> Result<CollectionVersion> {
        // Schema field names that don't exist in Go's JSON representation
        // When the "collection name" is actually one of these, produce Go-compatible
        // JSON patch errors instead of "adding collections" errors.
        const SCHEMA_FIELDS: &[&str] = &[
            "EncryptedIndexes",
            "VectorEmbeddings",
            "Indexes",
            "Fields",
            "Policy",
        ];

        // Check if the "collection name" is actually a schema field
        if SCHEMA_FIELDS.contains(&collection_name) {
            if let serde_json::Value::Array(ops) = patch_ops {
                for op in ops {
                    let operation = op.get("op").and_then(|v| v.as_str());
                    let value = op.get("value");

                    match operation {
                        Some("add") => {
                            // For add with array value, Go produces unmarshal error
                            if value.map(|v| v.is_array()).unwrap_or(false) {
                                return Err(Error::InvalidPatch(
                                    "cannot unmarshal array into Go value".to_string(),
                                ));
                            }
                            return Err(Error::InvalidPatch(
                                "cannot unmarshal array into Go value".to_string(),
                            ));
                        }
                        Some("remove") => {
                            return Err(Error::InvalidPatch(
                                "unable to remove nonexistent key".to_string(),
                            ));
                        }
                        Some("replace") => {
                            return Err(Error::InvalidPatch("doc is missing key".to_string()));
                        }
                        _ => {}
                    }
                }
            }
        }

        // Try to extract the actual collection name from the patch value's Name field.
        // This handles cases like path "/-" where the collection name in the path is "-"
        // but the actual name is in the value object.
        let effective_name = if let serde_json::Value::Array(ops) = patch_ops {
            ops.iter()
                .find_map(|op| {
                    op.get("value")
                        .and_then(|v| v.get("Name"))
                        .and_then(|n| n.as_str())
                        .map(String::from)
                })
                .unwrap_or_else(|| collection_name.to_string())
        } else {
            collection_name.to_string()
        };

        if let serde_json::Value::Array(ops) = patch_ops {
            // Look for move/copy operations to determine if this is a routing issue
            for op in ops {
                let operation = op.get("op").and_then(|v| v.as_str());
                let from_raw = op.get("from").and_then(|v| v.as_str());

                match operation {
                    Some("add") => {
                        // Check if the patch targets a sub-path within the unknown collection.
                        // If so, it's "doc is missing path" (the collection doesn't exist).
                        // If it targets the root (no sub-path), it's "adding collections not supported".
                        let raw_path = op.get("path").and_then(|v| v.as_str()).unwrap_or("");
                        let trimmed = raw_path.trim_start_matches('/');
                        let has_subpath = trimmed.contains('/');
                        if has_subpath {
                            return Err(Error::InvalidPatch(
                                "add operation does not apply: doc is missing path".to_string(),
                            ));
                        }
                        return Err(Error::InvalidPatch(format!(
                            "adding collections via patch is not supported. Name: {}",
                            effective_name,
                        )));
                    }
                    Some("copy") | Some("replace") => {
                        return Err(Error::InvalidPatch(format!(
                            "adding collections via patch is not supported. Name: {}",
                            effective_name,
                        )));
                    }
                    Some("move") => {
                        // Collection-level move is a no-op - find source and return unchanged
                        if let Some(from) = from_raw {
                            let source_name =
                                from.trim_start_matches('/').split('/').next().unwrap_or("");
                            if let Some(source_col) = self.get_collection(source_name)? {
                                return Ok(source_col.schema().clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // No recognized operation - adding collections via patch is not supported
        Err(Error::InvalidPatch(format!(
            "adding collections via patch is not supported. Name: {}",
            effective_name,
        )))
    }

    /// Substitute field names for indices in paths like /Fields/<name>.
    /// Go DefraDB allows using field names as array indices in patches.
    pub(crate) fn substitute_field_name_in_path(
        path: &str,
        schema_json: &serde_json::Value,
    ) -> String {
        // Check if path contains /Fields/ followed by a non-numeric segment
        if !path.contains("/Fields/") {
            return path.to_string();
        }

        let segments: Vec<&str> = path.split('/').collect();
        let mut result_segments: Vec<String> = Vec::new();

        let mut i = 0;
        while i < segments.len() {
            let segment = segments[i];

            if segment == "Fields" && i + 1 < segments.len() {
                result_segments.push("Fields".to_string());
                i += 1;

                let next_segment = segments[i];
                // Check if next segment is a number (already an index)
                if next_segment.parse::<usize>().is_ok() || next_segment == "-" {
                    result_segments.push(next_segment.to_string());
                } else {
                    // It's a field name - look up the index in the existing Fields array
                    if let Some(fields) = schema_json.get("Fields").and_then(|f| f.as_array()) {
                        let mut found = false;
                        for (idx, field) in fields.iter().enumerate() {
                            if let Some(name) = field.get("Name").and_then(|n| n.as_str()) {
                                if name == next_segment {
                                    result_segments.push(idx.to_string());
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            // Field name not found in existing fields - treat as append
                            // (Go interprets unknown field names as new field additions)
                            result_segments.push("-".to_string());
                        }
                    } else {
                        result_segments.push("-".to_string());
                    }
                }
            } else {
                result_segments.push(segment.to_string());
            }
            i += 1;
        }

        result_segments.join("/")
    }
}
