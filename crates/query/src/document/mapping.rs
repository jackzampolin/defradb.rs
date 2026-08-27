//! Document mapping for query result field positioning

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::doc::Doc;

/// Index of the DocID field in a document (always first)
pub const DOC_ID_FIELD_INDEX: usize = 0;

/// A key that should be rendered into the document output
#[derive(Debug, Clone, PartialEq)]
pub struct RenderKey {
    /// The field index to be rendered
    pub index: usize,
    /// The key by which the field contents should be rendered
    pub key: String,
}

impl RenderKey {
    pub fn new(index: usize, key: impl Into<String>) -> Self {
        Self {
            index,
            key: key.into(),
        }
    }
}

/// Type information for the object (for polymorphic queries)
#[derive(Debug, Clone, PartialEq)]
struct TypeInfo {
    /// The index at which the type name is held
    index: usize,
    /// The name of the host type
    name: String,
}

/// Document mapping for query results.
///
/// Maps field names to indexes in the document's fields array,
/// tracks which fields to render, and manages child mappings for nested objects.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DocumentMapping {
    /// Type information for the object (if provided)
    type_info: Option<TypeInfo>,

    /// The set of fields that should be rendered.
    ///
    /// Fields not in this collection will not be rendered to the consumer.
    pub render_keys: Vec<RenderKey>,

    /// The set of fields available using this mapping.
    ///
    /// If a field-name is not in this collection, it essentially doesn't exist.
    /// Multiple fields may exist for any given name (e.g., aliases).
    indexes_by_name: HashMap<String, Vec<usize>>,

    /// The next index available for use (also = number of fields)
    next_index: usize,

    /// Child mappings for nested objects.
    ///
    /// Indexes correspond to field indexes. None if the field is unmappable.
    pub child_mappings: Vec<Option<Box<DocumentMapping>>>,
}

impl DocumentMapping {
    /// Create a new empty document mapping
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next available index (also the number of fields)
    pub fn next_index(&self) -> usize {
        self.next_index
    }

    /// Get the number of fields in this mapping
    pub fn field_count(&self) -> usize {
        self.next_index
    }

    /// Add a field index with the given name
    pub fn add(&mut self, index: usize, name: impl Into<String>) {
        let name = name.into();
        self.indexes_by_name.entry(name).or_default().push(index);
        if index >= self.next_index {
            self.next_index = index + 1;
        }
    }

    /// Get the first index for a field name, if it exists
    pub fn first_index_of_name(&self, name: &str) -> Option<usize> {
        self.indexes_by_name
            .get(name)
            .and_then(|v| v.first().copied())
    }

    /// Get all indexes for a field name
    pub fn indexes_of_name(&self, name: &str) -> Option<&[usize]> {
        self.indexes_by_name.get(name).map(|v| v.as_slice())
    }

    /// Check if a field name exists in the mapping
    pub fn has_field(&self, name: &str) -> bool {
        self.indexes_by_name.contains_key(name)
    }

    /// Add a render key for the given index
    pub fn add_render_key(&mut self, index: usize, key: impl Into<String>) {
        self.render_keys.push(RenderKey::new(index, key));
    }

    /// Iterate over all field name → index entries
    pub fn indexes_by_name_iter(&self) -> impl Iterator<Item = (&str, &[usize])> {
        self.indexes_by_name
            .iter()
            .map(|(name, indexes)| (name.as_str(), indexes.as_slice()))
    }

    /// Try to find the name of a field by its index
    pub fn try_find_name_from_index(&self, target_index: usize) -> Option<&str> {
        for (name, indexes) in &self.indexes_by_name {
            if indexes.contains(&target_index) {
                return Some(name);
            }
        }
        None
    }

    /// Try to find an index from a render key
    pub fn try_find_index_from_render_key(&self, key: &str) -> Option<usize> {
        self.render_keys
            .iter()
            .find(|rk| rk.key == key)
            .map(|rk| rk.index)
    }

    /// Set the type name for this mapping (for __typename field)
    pub fn set_type_name(&mut self, type_name: impl Into<String>) {
        let index = self.next_index;
        self.add(index, "__typename");
        self.type_info = Some(TypeInfo {
            index,
            name: type_name.into(),
        });
    }

    /// Get the type name if set
    pub fn type_name(&self) -> Option<&str> {
        self.type_info.as_ref().map(|ti| ti.name.as_str())
    }

    /// Set a child mapping at the given index
    pub fn set_child_at(&mut self, index: usize, child_mapping: DocumentMapping) {
        if index >= self.child_mappings.len() {
            self.child_mappings.resize(index + 1, None);
        }
        self.child_mappings[index] = Some(Box::new(child_mapping));
    }

    /// Get a child mapping at the given index
    pub fn child_at(&self, index: usize) -> Option<&DocumentMapping> {
        self.child_mappings
            .get(index)
            .and_then(|opt| opt.as_ref().map(|b| b.as_ref()))
    }

    /// Get a mutable reference to a child mapping at the given index
    pub fn child_at_mut(&mut self, index: usize) -> Option<&mut DocumentMapping> {
        self.child_mappings
            .get_mut(index)
            .and_then(|opt| opt.as_mut().map(|b| b.as_mut()))
    }

    /// Render a document to a JSON object using this mapping's render keys.
    ///
    /// Iterates over render_keys and extracts the corresponding values from the document
    /// to build a JSON object suitable for output.
    ///
    /// Missing fields are rendered as `null` to match GraphQL conventions and provide
    /// consistent output structure regardless of data presence.
    ///
    /// Special handling for `__typename`: if the type_info is set and the render_key
    /// matches the __typename index, the stored type name is used instead of looking
    /// up the value in the document.
    ///
    /// Special handling for `_deleted`: the deleted status is stored as a flag on the
    /// Doc struct, not in the fields array, so we use `doc.is_deleted()` to get the value.
    pub fn render_doc_to_json(&self, doc: &Doc) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Get the _deleted index if present in mapping
        let deleted_index = self.first_index_of_name("_deleted");

        for render_key in &self.render_keys {
            // Check for _deleted special handling - the deleted status is stored
            // as a flag on the Doc, not in the fields array
            let value = if Some(render_key.index) == deleted_index && render_key.key == "_deleted" {
                JsonValue::Bool(doc.is_deleted())
            } else if let Some(ref type_info) = self.type_info {
                // Check for __typename special handling
                if render_key.index == type_info.index {
                    // Return the stored type name for __typename
                    JsonValue::String(type_info.name.clone())
                } else {
                    doc.get(render_key.index)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                }
            } else {
                // Use null for missing fields to match GraphQL conventions
                doc.get(render_key.index)
                    .cloned()
                    .unwrap_or(JsonValue::Null)
            };
            obj.insert(render_key.key.clone(), value);
        }
        JsonValue::Object(obj)
    }

    /// Clone without render keys (for subqueries)
    pub fn clone_without_render(&self) -> Self {
        let mut result = Self {
            type_info: self.type_info.clone(),
            render_keys: Vec::new(),
            indexes_by_name: self.indexes_by_name.clone(),
            next_index: self.next_index,
            child_mappings: Vec::with_capacity(self.child_mappings.len()),
        };

        for child in &self.child_mappings {
            result
                .child_mappings
                .push(child.as_ref().map(|c| Box::new(c.clone_without_render())));
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mapping() {
        let mapping = DocumentMapping::new();
        assert_eq!(mapping.next_index(), 0);
        assert!(mapping.render_keys.is_empty());
    }

    #[test]
    fn test_add_field() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "age");

        assert_eq!(mapping.next_index(), 3);
        assert_eq!(mapping.first_index_of_name("_docID"), Some(0));
        assert_eq!(mapping.first_index_of_name("name"), Some(1));
        assert_eq!(mapping.first_index_of_name("age"), Some(2));
        assert_eq!(mapping.first_index_of_name("unknown"), None);
    }

    #[test]
    fn test_multiple_indexes_same_name() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "name"); // alias

        assert_eq!(mapping.indexes_of_name("name"), Some(&[1, 2][..]));
        assert_eq!(mapping.first_index_of_name("name"), Some(1));
    }

    #[test]
    fn test_find_name_from_index() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");

        assert_eq!(mapping.try_find_name_from_index(0), Some("_docID"));
        assert_eq!(mapping.try_find_name_from_index(1), Some("name"));
        assert_eq!(mapping.try_find_name_from_index(99), None);
    }

    #[test]
    fn test_render_keys() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.render_keys.push(RenderKey::new(0, "_docID"));
        mapping.render_keys.push(RenderKey::new(1, "name"));

        assert_eq!(mapping.try_find_index_from_render_key("_docID"), Some(0));
        assert_eq!(mapping.try_find_index_from_render_key("name"), Some(1));
        assert_eq!(mapping.try_find_index_from_render_key("unknown"), None);
    }

    #[test]
    fn test_type_name() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.set_type_name("User");

        assert_eq!(mapping.type_name(), Some("User"));
        assert!(mapping.has_field("__typename"));
    }

    #[test]
    fn test_child_mappings() {
        let mut parent = DocumentMapping::new();
        parent.add(0, "_docID");
        parent.add(1, "author");

        let mut child = DocumentMapping::new();
        child.add(0, "_docID");
        child.add(1, "name");

        parent.set_child_at(1, child);

        assert!(parent.child_at(0).is_none());
        let child_ref = parent.child_at(1).unwrap();
        assert_eq!(child_ref.first_index_of_name("name"), Some(1));
    }

    #[test]
    fn test_clone_without_render() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.render_keys.push(RenderKey::new(0, "_docID"));
        mapping.render_keys.push(RenderKey::new(1, "name"));

        let cloned = mapping.clone_without_render();

        assert!(cloned.render_keys.is_empty());
        assert_eq!(cloned.first_index_of_name("_docID"), Some(0));
        assert_eq!(cloned.first_index_of_name("name"), Some(1));
    }
}
