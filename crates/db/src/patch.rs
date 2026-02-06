//! Schema patching operations for DefraDB.
//!
//! This module implements JSON Patch (RFC 6902) operations for modifying
//! collection schemas. It provides Go DefraDB compatible patching behavior
//! including field addition, removal, and schema version management.

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::json_patch::{
    extract_field_name_from_path, json_pointer_get, json_pointer_remove, json_pointer_set,
    JsonPatchError,
};
use schema::{CollectionSource, CollectionVersion};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey, CollectionVersionKey};
use tracing::instrument;

impl<S: Store> crate::database::DB<S> {
    /// Apply a JSON Patch to a collection schema.
    ///
    /// This method takes a JSON Patch document (RFC 6902) and applies it to
    /// the collection's schema, creating a new schema version.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection to patch (can also be version ID)
    /// * `patch` - JSON array of patch operations
    ///
    /// # Patch Operations
    ///
    /// Supported operations:
    /// - `add` - Add a new field or value
    /// - `remove` - Remove a field or value
    /// - `replace` - Replace an existing value
    /// - `test` - Test that a value exists
    /// - `copy` - Copy a value from one location to another
    /// - `move` - Move a value from one location to another
    ///
    /// # Returns
    ///
    /// The updated collection version (with new version_id).
    ///
    /// # Errors
    ///
    /// - `CollectionNotFound` if the collection doesn't exist
    /// - `InvalidPatch` if the patch is invalid or cannot be applied
    /// - `Schema` if the resulting schema is invalid
    #[instrument(skip(self, patch), fields(collection = %collection_name), name = "db.patch_collection")]
    pub async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> Result<CollectionVersion> {
        // Parse the patch early - needed for both collection lookup fallbacks and processing
        let patch_ops: serde_json::Value =
            serde_json::from_str(patch).map_err(|e| Error::InvalidPatch(e.to_string()))?;

        // Get the current schema - try by name first, then by version ID (including KV store),
        // then check for collection-level move/copy targeting a non-existent collection
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => {
                // Try looking up by version ID - search both cache and KV store
                match self
                    .get_collection_by_version_id_full(collection_name)
                    .await?
                {
                    Some(c) => c,
                    None => {
                        // Collection not found by name or version ID.
                        // Check if the patch is a collection-level move/copy where the
                        // "path" targets a non-existent collection (e.g., move /Users → /Books)
                        return self
                            .handle_unknown_collection_patch(collection_name, &patch_ops)
                            .await;
                    }
                }
            }
        };

        let old_schema = collection.schema().clone();
        let actual_name = old_schema.name.clone();
        let old_version_id = old_schema.version_id.clone();
        let collection_id = old_schema.collection_id.clone();

        // Collect known collection names for Kind validation
        let known_collection_names: Vec<String> = self
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Apply the patch to the schema JSON
        let mut schema_json = serde_json::to_value(&old_schema).map_err(|e| {
            Error::Serialization(format!("failed to serialize schema to JSON: {}", e))
        })?;

        // Normalize JSON to match Go's serialization format before applying patches.
        // Go always serializes struct fields (null for nil pointers, [] for nil slices),
        // but Rust's skip_serializing_if omits them. Patches targeting these paths
        // need the keys to exist for replace/remove operations to work correctly,
        // and for validators to run instead of json_pointer errors.
        // Note: EncryptedIndexes is NOT pre-populated because Go doesn't expose
        // it in the JSON representation - patches targeting it should fail.
        if let serde_json::Value::Object(ref mut map) = schema_json {
            for key in &["Indexes", "VectorEmbeddings"] {
                map.entry(key.to_string())
                    .or_insert(serde_json::Value::Array(vec![]));
            }
            for key in &["CollectionSet", "Query", "PreviousVersion", "Policy"] {
                map.entry(key.to_string())
                    .or_insert(serde_json::Value::Null);
            }
        }

        // Apply JSON patch operations
        // Go DefraDB embeds collection name in patch paths: /CollectionName/Fields/-
        // We need to strip the collection name prefix to get paths relative to schema.
        // Patches may use the collection name, actual name, or version ID as prefix.
        // Build a list of all recognized prefixes to try stripping.
        let mut strip_prefixes: Vec<String> = vec![format!("/{}/", collection_name)];
        if actual_name != collection_name {
            strip_prefixes.push(format!("/{}/", actual_name));
        }
        if old_version_id != collection_name && old_version_id != actual_name {
            strip_prefixes.push(format!("/{}/", old_version_id));
        }

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
                    raw_path.map(|p| Self::strip_collection_prefix(p, &strip_prefixes));

                // Extract field name from path before substitution (for name mismatch validation)
                let field_name_from_path = stripped_path
                    .as_deref()
                    .and_then(extract_field_name_from_path);

                // Go compatibility: substitute field names for indices in /Fields/<name> paths
                let path =
                    stripped_path.map(|p| Self::substitute_field_name_in_path(&p, &schema_json));

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
                                .unwrap_or(&actual_name);
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
                            // Known CIDs proceed - sources/ownership validation
                            // is handled by definition_validation post-patch.
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
                                &known_collection_names,
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
                                        &known_collection_names,
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

                        if let Err(e) = json_pointer_set(&mut schema_json, path, value) {
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
                            // Go compatibility: For top-level keys that don't exist (like
                            // EncryptedIndexes), produce Go-compatible error message.
                            let is_top_level_path =
                                path.starts_with('/') && !path[1..].contains('/');
                            if is_top_level_path {
                                let key = &path[1..];
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
                            json_pointer_remove(&mut schema_json, path)?;
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
                        let actual_value = json_pointer_get(&schema_json, path);

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
                        let from_path =
                            Self::substitute_field_name_in_path(from_path, &schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(&from_path, &strip_prefixes);

                        // Get the value to copy. First try current schema, then
                        // cross-collection: Go applies patches against a global dict
                        // of all collections, so "from" can reference other collections.
                        let value_to_copy = json_pointer_get(&schema_json, &from_path)
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
                        json_pointer_set(&mut schema_json, path, value_to_copy)?;
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
                        let from_path =
                            Self::substitute_field_name_in_path(from_path, &schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(&from_path, &strip_prefixes);

                        // Get the value to move
                        let value_to_move =
                            json_pointer_get(&schema_json, &from_path).ok_or_else(|| {
                                Error::InvalidPatch(format!("path not found: {}", from_path))
                            })?;

                        // Remove from source first
                        json_pointer_remove(&mut schema_json, &from_path)?;

                        // Set at destination
                        json_pointer_set(&mut schema_json, path, value_to_move)?;
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

        // Handle in-place updates (deactivation, IsActive-only, or PreviousVersion/Transform-only).
        // These don't create a new schema version - they update the existing one.
        let is_isactive_only_change = is_active_explicitly_set
            && new_schema.fields == old_schema.fields
            && new_schema.name == old_schema.name;

        // Check if only PreviousVersion/Transform changed (lens migration linking).
        // This is an in-place update that adds a migration transform to an existing version.
        let is_transform_only_change = !is_deactivation
            && !is_active_explicitly_set
            && new_schema.fields == old_schema.fields
            && new_schema.name == old_schema.name
            && new_schema.is_active == old_schema.is_active
            && new_schema.previous_version != old_schema.previous_version;

        // Check if only metadata changed (VectorEmbeddings, Indexes, IsMaterialized, etc.)
        // without field or name changes. Go treats these as in-place updates.
        let is_metadata_only_change = !is_deactivation
            && !is_active_explicitly_set
            && !is_transform_only_change
            && new_schema.fields == old_schema.fields
            && new_schema.name == old_schema.name
            && new_schema.is_active == old_schema.is_active
            && new_schema.previous_version == old_schema.previous_version;

        if is_deactivation
            || is_isactive_only_change
            || is_transform_only_change
            || is_metadata_only_change
        {
            if is_deactivation {
                new_schema.is_active = false;
            }
            // Keep original version_id
            new_schema.version_id = old_version_id.clone();
            // For IsActive-only, metadata-only, or deactivation, restore original previous_version.
            // For Transform-only changes, keep the new previous_version (contains the transform).
            if !is_transform_only_change {
                new_schema.previous_version = old_schema.previous_version.clone();
            }

            // Validate: can't remove a version that is a dependency of another version
            // This check runs always for deactivation (even if already inactive),
            // matching Go's validateCollectionDoesNotHaveHigherVersion
            if is_deactivation {
                let all_versions = self.get_all_collection_versions().await?;
                for other in &all_versions {
                    if let Some(ref prev) = other.previous_version {
                        if prev.source_collection_id == old_version_id {
                            return Err(Error::InvalidPatch(
                                "cannot delete a version that is used by a newer version, first delete the new version".to_string(),
                            ));
                        }
                    }
                }
            }

            // Validate: can't remove a collection that has documents (only on active→inactive)
            if !new_schema.is_active && old_schema.is_active {
                let has_data = self.collection_has_data(&collection_id).await?;
                if has_data {
                    return Err(Error::InvalidPatch(
                        "cannot delete a collection that has documents, first delete the documents and then delete the version".to_string(),
                    ));
                }
            }

            // Run cross-collection validators to catch issues like multiple active versions
            let all_existing = self.get_all_collection_versions().await?;
            let new_collections: Vec<CollectionVersion> = all_existing
                .iter()
                .filter(|c| c.version_id != old_version_id)
                .cloned()
                .chain(std::iter::once(new_schema.clone()))
                .collect();
            crate::definition_validation::validate_collection_changes(
                &all_existing,
                &new_collections,
            )
            .map_err(Error::InvalidPatch)?;

            // Store the updated version
            let txn = self.new_txn(false).await?;
            {
                let systemstore = txn.systemstore()?;
                let key = CollectionKey::new(&old_version_id);
                let data = serde_json::to_vec(&new_schema).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to serialize updated schema version '{}': {}",
                        old_version_id, e
                    ))
                })?;
                systemstore
                    .set(&key.bytes(), &data)
                    .await
                    .map_err(Error::Storage)?;

                // Update name pointer based on activation state
                let name_key = CollectionNameKey::new(&actual_name);
                if new_schema.is_active {
                    systemstore
                        .set(&name_key.bytes(), old_version_id.as_bytes())
                        .await
                        .map_err(Error::Storage)?;
                } else {
                    systemstore
                        .delete(&name_key.bytes())
                        .await
                        .map_err(Error::Storage)?;
                }
            }
            txn.commit().await?;

            // Update cache
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during in-place update");
                Error::CacheUpdateFailedAfterCommit(actual_name.clone())
            })?;
            if new_schema.is_active {
                cache.insert(actual_name.clone(), Collection::new(new_schema.clone()));
            } else {
                cache.remove(&actual_name);
            }

            tracing::info!(
                collection = %actual_name,
                version = %old_version_id,
                is_active = new_schema.is_active,
                "Updated collection version in place"
            );

            return Ok(new_schema);
        }

        // --- Normal path: create a new schema version ---

        // Go compatibility: default new fields with CType::None to CType::LwwRegister.
        // Go's patchCollection does this in collection_define.go for new fields that
        // don't have an explicit CRDT type. This must happen before CID generation.
        {
            let old_field_names: std::collections::HashSet<&str> =
                old_schema.fields.iter().map(|f| f.name.as_str()).collect();
            for field in &mut new_schema.fields {
                if !old_field_names.contains(field.name.as_str())
                    && field.crdt_type == schema::CType::None
                {
                    field.crdt_type = schema::CType::LwwRegister;
                }
            }
        }

        // Run Go-compatible cross-collection validators (before schema validate() which
        // uses different error messages). These validators cover duplicate fields,
        // CRDT/kind compatibility, and all Go-specific patch constraints.
        let all_existing = self.get_all_collection_versions().await?;
        let new_collections: Vec<CollectionVersion> = all_existing
            .iter()
            .filter(|c| c.version_id != old_version_id)
            .cloned()
            .chain(std::iter::once(new_schema.clone()))
            .collect();
        crate::definition_validation::validate_collection_changes(&all_existing, &new_collections)
            .map_err(Error::InvalidPatch)?;

        // Also run schema-level validation for checks not covered by definition validators
        // (e.g., relation field requires relation_name, policy format validation)
        new_schema.validate()?;

        // Auto-create unique indexes for one-to-one relations added via patch.
        // This runs AFTER validation (which rejects index mutations on existing schemas)
        // but BEFORE CID generation (since indexes are part of the schema content).
        {
            // Go uses sequential IDs starting from the next available for this collection
            let schema_max_index_id = new_schema
                .indexes
                .iter()
                .map(|idx| idx.id)
                .max()
                .unwrap_or(0);
            let mut next_index_id = schema_max_index_id;

            let mut indexes_to_add = Vec::new();
            for field in &new_schema.fields {
                if !field.kind.is_relation() || field.kind.is_array() {
                    continue;
                }
                let rel_name = match field.relation_name.as_ref() {
                    Some(n) => n,
                    None => continue,
                };
                let other_col_id = match field.kind.relation_collection_id() {
                    Some(id) => id,
                    None => continue,
                };
                // Look up the other collection (may be the same collection for self-ref)
                let other_col = all_existing.iter().find(|c| {
                    (c.name == other_col_id || c.collection_id == other_col_id) && c.is_active
                });
                if let Some(other_col) = other_col {
                    let other_field =
                        other_col.field_by_relation(rel_name, &new_schema.name, &field.name);
                    let other_is_array = other_field.map(|f| f.kind.is_array()).unwrap_or(false);
                    // One-to-one: other side exists and is non-array, this field is primary
                    if !other_is_array && field.is_primary {
                        match new_schema.ensure_one_to_one_unique_index(&field.name, &mut || {
                            next_index_id += 1;
                            next_index_id
                        }) {
                            Ok(Some(index)) => indexes_to_add.push(index),
                            Ok(None) => {} // existing unique index is fine
                            Err(e) => return Err(Error::InvalidPatch(e.to_string())),
                        }
                    }
                }
            }
            for index in indexes_to_add {
                new_schema.indexes.push(index);
            }
        }

        // Read current heads from schema_heads (emulates Go's persistent headstore).
        // For branching patches (v1→v2 then v1→v3), the headstore tracks the latest
        // CID after v2, so v3 gets heads=[v2_cid] and priority=3, matching Go.
        let (collection_heads, collection_priority) = {
            let heads_map = self
                .schema_heads
                .read()
                .map_err(|_| Error::LockPoisoned("schema_heads lock poisoned".into()))?;
            match heads_map.get(&actual_name) {
                Some((heads, h)) => (heads.clone(), *h + 1),
                None => {
                    // Fallback: compute from version chain (for databases loaded from storage)
                    let versions_map: std::collections::HashMap<&str, &CollectionVersion> =
                        all_existing
                            .iter()
                            .map(|v| (v.version_id.as_str(), v))
                            .collect();
                    let mut depth = 0u64;
                    let mut current_id = old_schema.version_id.as_str();
                    while let Some(v) = versions_map.get(current_id) {
                        match &v.previous_version {
                            Some(prev) => {
                                depth += 1;
                                current_id = prev.source_collection_id.as_str();
                            }
                            None => break,
                        }
                    }
                    let version_depth = depth + 1;
                    let old_cid = cid::Cid::try_from(old_schema.version_id.as_str()).ok();
                    (old_cid.into_iter().collect(), version_depth + 1)
                }
            }
        };

        // Generate new version_id from schema content with headstore heads and priority
        let new_version_id = Self::generate_patch_version_id_with_heads(
            &mut new_schema,
            &old_schema,
            collection_priority,
            &collection_heads,
        );

        // Update new schema with version info
        new_schema.version_id = new_version_id.clone();

        // Update schema_heads with new version CID and priority
        if let Ok(new_cid) = cid::Cid::try_from(new_version_id.as_str()) {
            if let Ok(mut heads) = self.schema_heads.write() {
                heads.insert(actual_name.clone(), (vec![new_cid], collection_priority));
            }
        }

        // Check if a placeholder version exists with this ID (from pre-registered migration).
        // When set_migration is called before patch_collection, it creates a placeholder
        // with previous_version.transform set. We need to copy that transform to preserve
        // the migration link.
        let placeholder_transform = {
            let read_txn = self.new_txn(true).await?;
            let systemstore = read_txn.systemstore()?;
            let placeholder_key = CollectionKey::new(&new_version_id);
            match systemstore
                .get(&placeholder_key.bytes())
                .await
                .map_err(Error::Storage)?
            {
                Some(data) => {
                    let placeholder: CollectionVersion =
                        serde_json::from_slice(&data).map_err(|e| {
                            Error::Serialization(format!(
                                "failed to deserialize placeholder version: {}",
                                e
                            ))
                        })?;
                    tracing::debug!(
                        new_version_id = %new_version_id,
                        is_placeholder = placeholder.is_placeholder,
                        has_previous_version = placeholder.previous_version.is_some(),
                        transform = ?placeholder.previous_version.as_ref().and_then(|pv| pv.transform.as_ref()),
                        "patch_collection: found existing version"
                    );
                    if placeholder.is_placeholder {
                        // Found a placeholder - extract its transform
                        placeholder.previous_version.and_then(|pv| pv.transform)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };

        // Use placeholder transform if available, otherwise None
        new_schema.previous_version = Some(CollectionSource {
            source_collection_id: old_version_id.clone(),
            transform: placeholder_transform.clone(),
        });

        if placeholder_transform.is_some() {
            tracing::debug!(
                new_version = %new_version_id,
                transform_id = ?placeholder_transform,
                "Linked pre-registered migration from placeholder to new schema version"
            );
        }

        // Also check for pending migrations targeting this new version (in-memory fallback)
        {
            let pending = self.pending_migrations.read().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned");
                Error::LockPoisoned(
                    "pending migrations lock poisoned during patch_collection".into(),
                )
            })?;
            if let Some((_source_id, transform_id)) = pending.get(&new_version_id) {
                if let Some(ref mut prev) = new_schema.previous_version {
                    // Only override if we didn't already get a transform from the placeholder
                    if prev.transform.is_none() {
                        prev.transform = Some(transform_id.clone());
                        tracing::debug!(
                            new_version = %new_version_id,
                            transform_id = %transform_id,
                            "Linked pending migration to new schema version"
                        );
                    }
                }
            }
        }

        // Go compatibility: respect explicit IsActive=false in the patch, otherwise default to true.
        // When IsActive was explicitly set to false in the patch, preserve it.
        // When the new version is inactive, keep the old version active.
        if !is_active_explicitly_set {
            new_schema.is_active = true;
        }

        // Create old schema copy for storage. If new schema is active, mark old as inactive.
        // If new schema is inactive (explicit IsActive=false), old version stays active.
        let mut old_schema_inactive = old_schema.clone();
        if new_schema.is_active {
            old_schema_inactive.is_active = false;
        }

        tracing::info!(
            collection = %collection_name,
            old_version = %old_version_id,
            new_version = %new_version_id,
            field_count = new_schema.fields.len(),
            "Creating new schema version"
        );

        // Begin transaction to store all version data
        let txn = self.new_txn(false).await?;

        // Prepare serialized data before getting systemstore reference
        let old_version_key = CollectionKey::new(&old_version_id);
        let old_version_data = serde_json::to_vec(&old_schema_inactive).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize old schema version '{}': {}",
                old_version_id, e
            ))
        })?;
        let new_version_key = CollectionKey::new(&new_version_id);
        let new_version_data = serde_json::to_vec(&new_schema).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize new schema version '{}': {}",
                new_version_id, e
            ))
        })?;
        let name_key = CollectionNameKey::new(&actual_name);
        let version_index_key = CollectionVersionKey::new(&collection_id, &new_version_id);
        let old_version_index_key = CollectionVersionKey::new(&collection_id, &old_version_id);

        // Perform all writes in a scoped block so systemstore reference is dropped
        {
            let systemstore = txn.systemstore()?;

            // 1. Store old version at /collection/id/{old_version_id} with is_active = false
            systemstore
                .set(&old_version_key.bytes(), &old_version_data)
                .await
                .map_err(Error::Storage)?;

            // 2. Store new version at /collection/id/{new_version_id}
            systemstore
                .set(&new_version_key.bytes(), &new_version_data)
                .await
                .map_err(Error::Storage)?;

            // 3. Update /collection/name/{name} - only point to new version if it's active.
            // If new version is inactive, keep name pointing to old version (which stays active).
            if new_schema.is_active {
                systemstore
                    .set(&name_key.bytes(), new_version_id.as_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }

            // 4. Add version index at /collection/version/{collection_id}/{new_version_id}
            systemstore
                .set(&version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;

            // 5. Also ensure old version is in the version index (may already exist)
            systemstore
                .set(&old_version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;
        } // systemstore reference dropped here

        txn.commit().await?;

        // Clean up any pending migration that was linked to this version
        {
            let mut pending = self.pending_migrations.write().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned during cleanup");
                Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
            })?;
            pending.remove(&new_version_id);
        }

        // Update cache based on which version is active
        {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %collection_name,
                    "Collection cache lock poisoned during patch_collection update"
                );
                Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
            })?;
            if new_schema.is_active {
                // New version is active - cache it under the actual collection name
                // (not collection_name, which might be a version_id for branching patches)
                cache.insert(actual_name.clone(), Collection::new(new_schema.clone()));
            }
            // If new version is inactive, old version stays in cache (already there)
        }

        // After switching active versions, reindex if the new version's history has migrations
        if new_schema.is_active {
            if let Err(e) = self.maybe_reindex_on_version_switch(&actual_name).await {
                tracing::warn!(
                    error = %e,
                    collection = %actual_name,
                    "Failed to reindex after version switch"
                );
            }
        }

        Ok(new_schema)
    }

    /// Generate a version ID (CID) from schema content during patching.
    ///
    /// Matches Go DefraDB's saveBlocks() behavior:
    /// - Existing fields (present in old_schema) are SKIPPED entirely
    /// - Only NEW fields get CIDs generated with priority=1 (empty headstore)
    /// - The collection block uses headstore heads and priority
    fn generate_patch_version_id_with_heads(
        schema: &mut CollectionVersion,
        old_schema: &CollectionVersion,
        collection_priority: u64,
        collection_heads: &[cid::Cid],
    ) -> String {
        use cid::Cid;
        use sha2::{Digest, Sha256};

        // Build set of old field names for detecting which fields are new
        let old_field_names: std::collections::HashSet<&str> = old_schema
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .map(|f| f.name.as_str())
            .collect();

        // Go's saveBlocks skips fields that already have a FieldID (existing fields).
        // Only NEW fields (not in old_schema) get CID generation and DAGLink inclusion.
        // Go also skips secondary relation fields (those have empty FieldID in old schema too,
        // but Delta returns hasFieldChanged=false for them).
        let new_field_indices: Vec<usize> = {
            let mut indices: Vec<usize> = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    // New field: not in old schema and not a secondary relation
                    // (secondary relations have relation_name set and is_primary=false)
                    let is_new = !old_field_names.contains(f.name.as_str());
                    let is_secondary_relation = f.relation_name.is_some() && !f.is_primary;
                    is_new && !is_secondary_relation
                })
                .map(|(i, _)| i)
                .collect();
            // Sort: _docID first, then alphabetically
            indices.sort_by(|&a, &b| {
                let fa = &schema.fields[a];
                let fb = &schema.fields[b];
                if fa.name == "_docID" {
                    std::cmp::Ordering::Less
                } else if fb.name == "_docID" {
                    std::cmp::Ordering::Greater
                } else {
                    fa.name.cmp(&fb.name)
                }
            });
            indices
        };

        // Generate CIDs only for NEW fields with priority=1 (matching Go's empty headstore)
        let mut field_cids: Vec<Cid> = Vec::new();
        for &idx in &new_field_indices {
            let field = &schema.fields[idx];
            match schema::generate_field_cid_with_priority(field, 1) {
                Ok(cid) => {
                    schema.fields[idx].id = cid.to_string();
                    field_cids.push(cid);
                }
                Err(_e) => {}
            }
        }

        // Generate collection CID with headstore heads.
        // Go's Delta only includes name when it changed. For field-only patches, name=None.
        let name_changed = schema.name != old_schema.name;
        let collection_name = if name_changed {
            Some(schema.name.as_str())
        } else {
            None
        };
        match schema::generate_collection_cid_full(
            collection_name,
            &field_cids,
            collection_priority,
            collection_heads,
        ) {
            Ok(cid) => cid.to_string(),
            Err(_) => {
                // Fallback to simple hash if CID generation fails
                let mut hasher = Sha256::new();
                hasher.update(b"version:");
                hasher.update(schema.name.as_bytes());
                for field in &schema.fields {
                    hasher.update(field.name.as_bytes());
                    hasher.update(field.id.as_bytes());
                }
                let hash = hasher.finalize();
                format!(
                    "v{:x}",
                    &hash[..8].iter().fold(0u64, |acc, &b| (acc << 8) | b as u64)
                )
            }
        }
    }

    /// Validate a Kind value in a patch field addition.
    /// Returns error if the Kind is an unsupported numeric value or unknown string.
    fn validate_patch_field_kind(
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
    fn strip_collection_prefix(path: &str, prefixes: &[String]) -> String {
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
    async fn handle_unknown_collection_patch(
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

    /// Helper: Substitute field names for indices in paths like /Fields/<name>
    /// Go DefraDB allows using field names as array indices in patches.
    fn substitute_field_name_in_path(path: &str, schema_json: &serde_json::Value) -> String {
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
