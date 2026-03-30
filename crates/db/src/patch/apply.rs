use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Apply RFC 6902 patch operations to a schema JSON value.
    ///
    /// Processes each operation (add, replace, remove, test, copy, move),
    /// handles Go compatibility quirks (collection name prefixes, field name
    /// substitution, Kind validation), and auto-generates FieldIDs.
    ///
    /// Returns `(is_deactivation, is_active_explicitly_set)` flags for the caller.
    pub(crate) async fn apply_patch_operations(
        &self,
        patch_ops: serde_json::Value,
        schema_json: &mut serde_json::Value,
        strip_prefixes: &[String],
        known_collection_names: &[String],
        old_schema: &CollectionVersion,
        collection_id: &str,
    ) -> Result<(bool, bool)> {
        // Track whether the patch deactivates this collection or explicitly changes IsActive.
        // These require in-place updates rather than new version creation.
        let mut is_deactivation = false;
        let mut is_active_explicitly_set = false;

        if let serde_json::Value::Array(ops) = patch_ops {
            for op in ops {
                let operation = op.get("op").and_then(|v| v.as_str());
                let raw_path = op.get("path").and_then(|v| v.as_str());
                let value = op.get("value");

                // Strip collection name/version prefix from path if present (Go compatibility)
                let stripped_path =
                    raw_path.map(|p| Self::strip_collection_prefix(p, strip_prefixes));

                // Extract field name from path before substitution (for name mismatch validation)
                let field_name_from_path = stripped_path
                    .as_deref()
                    .and_then(extract_field_name_from_path);

                // Go compatibility: substitute field names for indices in /Fields/<name> paths
                let path =
                    stripped_path.map(|p| Self::substitute_field_name_in_path(&p, schema_json));

                match (operation, path.as_deref()) {
                    (Some("replace"), Some(path)) | (Some("add"), Some(path)) => {
                        let mut value = value
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'value' for operation at {}",
                                    path
                                ))
                            })?
                            .clone();

                        // Go compatibility: root-level add/replace is "adding collections"
                        if path == "/" {
                            let name = value
                                .as_object()
                                .and_then(|m| m.get("Name"))
                                .and_then(|n| n.as_str())
                                .unwrap_or(&old_schema.name);
                            return Err(Error::InvalidPatch(format!(
                                "adding collections via patch is not supported. Name: {}",
                                name
                            )));
                        }

                        // Track explicit IsActive changes for in-place update handling
                        if path == "/IsActive" {
                            is_active_explicitly_set = true;
                        }

                        // Go compatibility: validate VersionID replacement
                        if path.ends_with("/VersionID") {
                            let version_id_str = value.as_str().unwrap_or("");
                            if version_id_str.is_empty() {
                                return Err(Error::InvalidPatch(
                                    "collection ID cannot be empty".to_string(),
                                ));
                            }
                            // Validate CID format
                            if cid::Cid::try_from(version_id_str).is_err() {
                                return Err(Error::InvalidPatch(format!(
                                    "invalid cid: selected encoding not supported. VersionID: {}",
                                    version_id_str
                                )));
                            }
                            // Check if this CID exists as a known collection version
                            let all_versions =
                                self.get_all_collection_versions().await.unwrap_or_default();
                            let is_known =
                                all_versions.iter().any(|c| c.version_id == version_id_str);
                            if !is_known {
                                return Err(Error::InvalidPatch(
                                    "unknown CID, collection ids cannot be manually defined"
                                        .to_string(),
                                ));
                            }
                            // Replacing a VersionID to point at an existing version
                            // constitutes a source redefinition. Go rejects this because
                            // it changes the version's PreviousVersion pointer.
                            // Check if the target belongs to the same collection root.
                            let target_version =
                                all_versions.iter().find(|c| c.version_id == version_id_str);
                            if let Some(target) = target_version {
                                if target.collection_id == collection_id {
                                    return Err(Error::InvalidPatch(
                                        "collection sources cannot be added or removed."
                                            .to_string(),
                                    ));
                                } else {
                                    return Err(Error::InvalidPatch(
                                        "collection source must belong to host collection."
                                            .to_string(),
                                    ));
                                }
                            }
                        }

                        // Go compatibility: validate Kind value when replacing a field's Kind directly
                        // e.g., replace /Fields/2/Kind with "NotAValidKind"
                        if path.ends_with("/Kind") && path.contains("/Fields/") {
                            // Extract field name from the path for error message
                            // Path is like /Fields/2/Kind, we need to get the name of field at index 2
                            let field_index_str = path
                                .trim_start_matches("/Fields/")
                                .split('/')
                                .next()
                                .unwrap_or("");
                            let field_name = if let Ok(idx) = field_index_str.parse::<usize>() {
                                schema_json
                                    .get("Fields")
                                    .and_then(|f| f.as_array())
                                    .and_then(|arr| arr.get(idx))
                                    .and_then(|f| f.get("Name"))
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("")
                            } else {
                                field_index_str
                            };
                            Self::validate_patch_field_kind(
                                &value,
                                field_name,
                                known_collection_names,
                            )?;
                        }

                        // Go compatibility: validate and auto-generate FieldID when adding new fields
                        // If path ends with /Fields/- or /Fields/<n> and value has Name but no FieldID
                        if path.contains("/Fields/") {
                            if let serde_json::Value::Object(ref mut map) = value {
                                // Validate field name matches path index name (Go compatibility)
                                if let Some(ref path_name) = field_name_from_path {
                                    if let Some(value_name) =
                                        map.get("Name").and_then(|n| n.as_str())
                                    {
                                        if !value_name.is_empty()
                                            && value_name != path_name.as_str()
                                        {
                                            return Err(Error::InvalidPatch(format!(
                                                "the index used does not match the given name. index: {}, name: {}",
                                                path_name, value_name
                                            )));
                                        }
                                    }
                                    // If value doesn't have Name, set it from the path
                                    if !map.contains_key("Name") {
                                        map.insert(
                                            "Name".to_string(),
                                            serde_json::Value::String(path_name.clone()),
                                        );
                                    }
                                }

                                // Validate Kind value for new fields
                                if let Some(kind_val) = map.get("Kind") {
                                    Self::validate_patch_field_kind(
                                        kind_val,
                                        map.get("Name").and_then(|n| n.as_str()).unwrap_or(""),
                                        known_collection_names,
                                    )?;
                                }
                                if map.contains_key("Name") && !map.contains_key("FieldID") {
                                    // Find max existing FieldID to avoid collisions with gaps
                                    let max_field_id = schema_json
                                        .get("Fields")
                                        .and_then(|f| f.as_array())
                                        .map(|arr| {
                                            arr.iter()
                                                .filter_map(|f| f.get("FieldID"))
                                                .filter_map(|id| {
                                                    id.as_str()
                                                        .and_then(|s| s.parse::<u64>().ok())
                                                        .or_else(|| id.as_u64())
                                                })
                                                .max()
                                                .unwrap_or(0)
                                        })
                                        .unwrap_or(0);
                                    let field_id = (max_field_id + 1).to_string();
                                    map.insert("FieldID".to_string(), field_id.into());
                                }
                            }
                        }

                        // Go compatibility: For top-level array fields that don't exist (like
                        // EncryptedIndexes which isn't exposed in Go's JSON), produce Go-compatible
                        // error messages.
                        let is_top_level_path = path.starts_with('/') && !path[1..].contains('/');
                        if is_top_level_path {
                            let key = &path[1..];

                            // Encrypted indexes cannot be mutated via patch (Go parity).
                            if key == "EncryptedIndexes" {
                                return Err(Error::InvalidPatch(
                                    "collection encrypted indexes cannot be mutated".to_string(),
                                ));
                            }

                            let key_exists = schema_json
                                .as_object()
                                .map(|m| m.contains_key(key))
                                .unwrap_or(false);
                            if !key_exists {
                                // For add with array value, Go produces unmarshal error
                                if operation == Some("add") && value.is_array() {
                                    return Err(Error::InvalidPatch(
                                        "cannot unmarshal array into Go value".to_string(),
                                    ));
                                }
                                // The "-" key is the JSON Patch append operator. When used
                                // on an object (not array), Go applies it then fails during
                                // unmarshalling because "-" is not a valid struct field.
                                if operation == Some("add") && key == "-" {
                                    return Err(Error::InvalidPatch(
                                        "json: unknown field \"-\"".to_string(),
                                    ));
                                }
                                // For replace on non-existent key, Go produces "doc is missing key"
                                if operation == Some("replace") {
                                    return Err(Error::InvalidPatch(
                                        "doc is missing key".to_string(),
                                    ));
                                }
                            }
                        }

                        // RFC 6902: "add" inserts into arrays, "replace" replaces
                        let result = if operation == Some("replace") {
                            json_pointer_replace(schema_json, path, value)
                        } else {
                            json_pointer_set(schema_json, path, value)
                        };
                        if let Err(e) = result {
                            match &e {
                                JsonPatchError::PathNotFound(_)
                                | JsonPatchError::CannotNavigate(_) => {
                                    return Err(Error::InvalidPatch(
                                        "add operation does not apply: doc is missing path"
                                            .to_string(),
                                    ));
                                }
                                _ => return Err(e.into()),
                            }
                        }
                    }
                    (Some("remove"), Some(path)) => {
                        if path == "/" {
                            // Root-level remove = deactivate collection
                            is_deactivation = true;
                        } else {
                            let is_top_level_path =
                                path.starts_with('/') && !path[1..].contains('/');
                            if is_top_level_path {
                                let key = &path[1..];

                                // Encrypted indexes cannot be mutated via patch (Go parity).
                                if key == "EncryptedIndexes" {
                                    return Err(Error::InvalidPatch(
                                        "collection encrypted indexes cannot be mutated"
                                            .to_string(),
                                    ));
                                }
                                let key_exists = schema_json
                                    .as_object()
                                    .map(|m| m.contains_key(key))
                                    .unwrap_or(false);
                                if !key_exists {
                                    return Err(Error::InvalidPatch(
                                        "unable to remove nonexistent key".to_string(),
                                    ));
                                }
                            }
                            json_pointer_remove(schema_json, path)?;
                        }
                    }
                    (Some("test"), Some(path)) => {
                        // RFC 6902 "test" operation: verify value at path equals expected
                        let expected_value = value
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'value' for test operation at {}",
                                    path
                                ))
                            })?
                            .clone();

                        // Get the actual value at the path
                        let actual_value = json_pointer_get(schema_json, path);

                        // Compare: if path doesn't exist or values don't match, test fails
                        match actual_value {
                            Some(actual) if actual == expected_value => {
                                // Test passes - continue to next operation
                            }
                            _ => {
                                // Test fails - return error in Go-compatible format
                                // Include original path for context
                                let original_path = raw_path.unwrap_or(path);
                                return Err(Error::InvalidPatch(format!(
                                    "testing value {} failed: test failed",
                                    original_path
                                )));
                            }
                        }
                    }
                    (Some("copy"), Some(path)) => {
                        // RFC 6902 "copy" operation: copy value from "from" to "path"
                        let from_path =
                            op.get("from").and_then(|v| v.as_str()).ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'from' for copy operation at {}",
                                    path
                                ))
                            })?;

                        // Go compatibility: copying collection-level is not supported
                        // This includes copying to root "/" or to paths that would create new collections
                        // Detect by checking if path doesn't contain /Fields (field-level operations)
                        if path == "/"
                            || (!path.contains("/Fields")
                                && !path.contains("/Name")
                                && !path.contains("/IsActive"))
                        {
                            // Extract the target name from the raw path for the error message
                            let target_name = raw_path
                                .and_then(|p| {
                                    let p = p.trim_start_matches('/');
                                    p.split('/').next()
                                })
                                .unwrap_or("Unknown");
                            return Err(Error::InvalidPatch(format!(
                                "adding collections via patch is not supported. Name: {}",
                                target_name
                            )));
                        }

                        // Substitute field names in from path too
                        let from_path = Self::substitute_field_name_in_path(from_path, schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(&from_path, strip_prefixes);

                        // Get the value to copy. First try current schema, then
                        // cross-collection: Go applies patches against a global dict
                        // of all collections, so "from" can reference other collections.
                        let value_to_copy = json_pointer_get(schema_json, &from_path)
                            .or_else(|| {
                                let trimmed = from_path.trim_start_matches('/');
                                let first_slash = trimmed.find('/');
                                if let Some(pos) = first_slash {
                                    let other_name = &trimmed[..pos];
                                    let rest = &trimmed[pos..];
                                    if let Ok(Some(other_col)) = self.get_collection(other_name) {
                                        let other_schema = other_col.schema();
                                        if let Ok(other_json) = serde_json::to_value(other_schema) {
                                            return json_pointer_get(&other_json, rest);
                                        }
                                    }
                                }
                                None
                            })
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!("path not found: {}", from_path))
                            })?;

                        // When copying a field, clear the FieldID so it becomes a "new" field.
                        // Go DefraDB generates new FieldIDs for copied fields rather than
                        // treating them as mutations of the original.
                        let value_to_copy =
                            if path.contains("/Fields/") && value_to_copy.is_object() {
                                let mut v = value_to_copy;
                                if let Some(obj) = v.as_object_mut() {
                                    obj.remove("FieldID");
                                }
                                v
                            } else {
                                value_to_copy
                            };

                        // Set at destination
                        json_pointer_set(schema_json, path, value_to_copy)?;
                    }
                    (Some("move"), Some(path)) => {
                        // RFC 6902 "move" operation: move value from "from" to "path"
                        let from_path =
                            op.get("from").and_then(|v| v.as_str()).ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'from' for move operation at {}",
                                    path
                                ))
                            })?;

                        // Go compatibility: moving at collection-level is a no-op
                        // This includes moving to root "/" or paths that would move entire collections
                        // Detect by checking if path doesn't contain /Fields (field-level operations)
                        if path == "/"
                            || (!path.contains("/Fields")
                                && !path.contains("/Name")
                                && !path.contains("/IsActive"))
                        {
                            // Skip this operation - collection-level moves are no-ops
                            continue;
                        }

                        // Substitute field names in from path too
                        let from_path = Self::substitute_field_name_in_path(from_path, schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(&from_path, strip_prefixes);

                        // Get the value to move
                        let value_to_move =
                            json_pointer_get(schema_json, &from_path).ok_or_else(|| {
                                Error::InvalidPatch(format!("path not found: {}", from_path))
                            })?;

                        // Remove from source first
                        json_pointer_remove(schema_json, &from_path)?;

                        // Set at destination
                        json_pointer_set(schema_json, path, value_to_move)?;
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "unsupported or invalid patch operation: {:?}",
                            op
                        )));
                    }
                }
            }
        } else {
            return Err(Error::InvalidPatch(
                "patch must be an array of operations".to_string(),
            ));
        }

        // Go compatibility: auto-generate FieldID for any fields missing one
        // This handles cases where FieldID is removed (e.g., after copy operation)
        if let Some(fields) = schema_json.get_mut("Fields").and_then(|f| f.as_array_mut()) {
            // Find max existing FieldID
            let max_field_id: u64 = fields
                .iter()
                .filter_map(|f| f.get("FieldID"))
                .filter_map(|id| {
                    id.as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                        .or_else(|| id.as_u64())
                })
                .max()
                .unwrap_or(0);

            let mut next_id = max_field_id + 1;
            for field in fields.iter_mut() {
                if let serde_json::Value::Object(ref mut map) = field {
                    if !map.contains_key("FieldID")
                        || map.get("FieldID") == Some(&serde_json::Value::Null)
                        || map.get("FieldID").and_then(|v| v.as_str()) == Some("")
                    {
                        map.insert("FieldID".to_string(), next_id.to_string().into());
                        next_id += 1;
                    }
                }
            }
        }

        // Go compatibility: check for empty collection name before deserialization
        let name_value = schema_json.get("Name");
        match name_value {
            None | Some(serde_json::Value::Null) => {
                return Err(Error::InvalidPatch(
                    "collection name can't be empty".to_string(),
                ));
            }
            Some(serde_json::Value::String(s)) if s.is_empty() => {
                return Err(Error::InvalidPatch(
                    "collection name can't be empty".to_string(),
                ));
            }
            _ => {}
        }

        Ok((is_deactivation, is_active_explicitly_set))
    }
}
