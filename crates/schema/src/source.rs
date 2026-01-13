//! Collection source and set types.
//!
//! Matches Go's collection source types in client/collection_description.go

use serde::{Deserialize, Serialize};

/// Describes a collection's membership in a collection set.
/// Matches Go's CollectionSetDescription.
///
/// Collections form a set when they have circular relations at creation time.
/// For example: Book has relation to Author, Author has relation to Book.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionSetDescription {
    /// ID of the collection set.
    #[serde(rename = "CollectionSetID")]
    pub collection_set_id: String,

    /// This item's relative location within the set.
    /// Based on Name (lexographically ascending) at creation time.
    #[serde(rename = "RelativeID")]
    pub relative_id: i32,
}

impl CollectionSetDescription {
    /// Create a new collection set description.
    pub fn new(collection_set_id: impl Into<String>, relative_id: i32) -> Self {
        Self {
            collection_set_id: collection_set_id.into(),
            relative_id,
        }
    }
}

/// A data source from another collection instance.
/// Matches Go's CollectionSource.
///
/// Used to link schema versions together via migrations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionSource {
    /// Local identifier of the source collection version.
    #[serde(rename = "SourceCollectionID")]
    pub source_collection_id: String,

    /// Optional Lens transform ID to apply between versions.
    #[serde(rename = "Transform", default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<String>,
}

impl CollectionSource {
    /// Create a new collection source.
    pub fn new(source_collection_id: impl Into<String>) -> Self {
        Self {
            source_collection_id: source_collection_id.into(),
            transform: None,
        }
    }

    /// Add a transform to the source.
    pub fn with_transform(mut self, transform: impl Into<String>) -> Self {
        self.transform = Some(transform.into());
        self
    }
}

/// A data source from a query.
/// Matches Go's QuerySource.
///
/// Used for views that derive data from queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuerySource {
    /// The base query for this data source.
    /// Note: This is simplified - Go uses request.Select which is more complex.
    #[serde(rename = "Query")]
    pub query: serde_json::Value,

    /// Optional Lens transform ID.
    #[serde(rename = "Transform", default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<String>,
}

impl QuerySource {
    /// Create a new query source.
    pub fn new(query: serde_json::Value) -> Self {
        Self {
            query,
            transform: None,
        }
    }

    /// Add a transform to the query source.
    pub fn with_transform(mut self, transform: impl Into<String>) -> Self {
        self.transform = Some(transform.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_set_serialization() {
        let set = CollectionSetDescription::new("bafkrei123", 0);
        let json = serde_json::to_string(&set).unwrap();

        assert!(json.contains("\"CollectionSetID\""));
        assert!(json.contains("\"RelativeID\""));

        let parsed: CollectionSetDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(set, parsed);
    }

    #[test]
    fn test_collection_source_serialization() {
        let source = CollectionSource::new("bafkrei456").with_transform("lens-transform-1");

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains("\"SourceCollectionID\""));
        assert!(json.contains("\"Transform\""));

        let parsed: CollectionSource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_collection_source_without_transform() {
        let source = CollectionSource::new("bafkrei789");
        let json = serde_json::to_string(&source).unwrap();

        // Transform should be omitted when None
        assert!(!json.contains("Transform"));

        let parsed: CollectionSource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_query_source_serialization() {
        let query = serde_json::json!({
            "Name": "users",
            "Fields": ["name", "email"]
        });
        let source = QuerySource::new(query.clone());

        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains("\"Query\""));

        let parsed: QuerySource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }
}
