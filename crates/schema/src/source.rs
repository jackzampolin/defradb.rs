//! Collection source and set types.
//!
//! Matches Go's collection source types in client/collection_description.go

use serde::de::Error as _;
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    #[serde(rename = "Query", deserialize_with = "deserialize_query_select")]
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

/// Serialize a query using Go's `request.Select` JSON field order and defaults.
pub fn query_select_json_bytes(query: &serde_json::Value) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&GoSelect(query))
}

fn deserialize_query_select<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: Deserializer<'de>,
{
    let query = serde_json::Value::deserialize(deserializer)?;
    let bytes = query_select_json_bytes(&query).map_err(D::Error::custom)?;
    serde_json::from_slice(&bytes).map_err(D::Error::custom)
}

struct GoSelect<'a>(&'a serde_json::Value);

impl Serialize for GoSelect<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("query select must be an object"))?;
        let mut map = serializer.serialize_map(Some(12))?;
        map.serialize_entry(
            "Name",
            object
                .get("Name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.serialize_entry("Alias", &Nullable(object.get("Alias")))?;
        map.serialize_entry(
            "Fields",
            &GoSelections(object.get("Fields").and_then(serde_json::Value::as_array)),
        )?;
        map.serialize_entry("Limit", &Nullable(object.get("Limit")))?;
        map.serialize_entry("Offset", &Nullable(object.get("Offset")))?;
        map.serialize_entry("OrderBy", &Nullable(object.get("OrderBy")))?;
        map.serialize_entry("Filter", &Nullable(object.get("Filter")))?;
        map.serialize_entry("DocIDs", &Nullable(object.get("DocIDs")))?;
        map.serialize_entry(
            "CIDs",
            &Nullable(object.get("CIDs").or_else(|| object.get("CID"))),
        )?;
        map.serialize_entry("GroupBy", &Nullable(object.get("GroupBy")))?;
        map.serialize_entry(
            "ShowDeleted",
            &object
                .get("ShowDeleted")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        )?;
        map.serialize_entry(
            "IsEncrypted",
            &object
                .get("IsEncrypted")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        )?;
        map.end()
    }
}

struct GoSelections<'a>(Option<&'a Vec<serde_json::Value>>);

impl Serialize for GoSelections<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(fields) = self.0 else {
            return serializer.serialize_none();
        };
        let mut sequence = serializer.serialize_seq(Some(fields.len()))?;
        for field in fields {
            sequence.serialize_element(&GoSelection(field))?;
        }
        sequence.end()
    }
}

struct GoSelection<'a>(&'a serde_json::Value);

impl Serialize for GoSelection<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(name) = self.0.as_str() {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("Name", name)?;
            map.serialize_entry("Alias", &Option::<String>::None)?;
            return map.end();
        }

        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("query selection must be an object"))?;
        if object.contains_key("Fields") {
            return GoSelect(self.0).serialize(serializer);
        }
        if object.contains_key("Targets") {
            return GoAggregate(self.0).serialize(serializer);
        }
        if object.contains_key("Vector") {
            return GoSimilarity(self.0).serialize(serializer);
        }
        GoField(self.0).serialize(serializer)
    }
}

struct GoField<'a>(&'a serde_json::Value);

impl Serialize for GoField<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("query field must be an object"))?;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(
            "Name",
            object
                .get("Name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.serialize_entry("Alias", &Nullable(object.get("Alias")))?;
        map.end()
    }
}

struct GoAggregate<'a>(&'a serde_json::Value);

impl Serialize for GoAggregate<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("query aggregate must be an object"))?;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry(
            "Name",
            object
                .get("Name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.serialize_entry("Alias", &Nullable(object.get("Alias")))?;
        map.serialize_entry(
            "Targets",
            &GoAggregateTargets(object.get("Targets").and_then(serde_json::Value::as_array)),
        )?;
        map.end()
    }
}

struct GoAggregateTargets<'a>(Option<&'a Vec<serde_json::Value>>);

impl Serialize for GoAggregateTargets<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(targets) = self.0 else {
            return serializer.serialize_none();
        };
        let mut sequence = serializer.serialize_seq(Some(targets.len()))?;
        for target in targets {
            sequence.serialize_element(&GoAggregateTarget(target))?;
        }
        sequence.end()
    }
}

struct GoAggregateTarget<'a>(&'a serde_json::Value);

impl Serialize for GoAggregateTarget<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("aggregate target must be an object"))?;
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("Limit", &Nullable(object.get("Limit")))?;
        map.serialize_entry("Offset", &Nullable(object.get("Offset")))?;
        map.serialize_entry("OrderBy", &Nullable(object.get("OrderBy")))?;
        map.serialize_entry("Filter", &Nullable(object.get("Filter")))?;
        map.serialize_entry("GroupBy", &Nullable(object.get("GroupBy")))?;
        map.serialize_entry(
            "HostName",
            object
                .get("HostName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.serialize_entry("ChildName", &Nullable(object.get("ChildName")))?;
        map.end()
    }
}

struct GoSimilarity<'a>(&'a serde_json::Value);

impl Serialize for GoSimilarity<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| S::Error::custom("similarity selection must be an object"))?;
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry(
            "Name",
            object
                .get("Name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.serialize_entry("Alias", &Nullable(object.get("Alias")))?;
        map.serialize_entry("Vector", &Nullable(object.get("Vector")))?;
        map.serialize_entry(
            "Target",
            object
                .get("Target")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(""),
        )?;
        map.end()
    }
}

struct Nullable<'a>(Option<&'a serde_json::Value>);

impl Serialize for Nullable<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Some(value) => value.serialize(serializer),
            None => serializer.serialize_none(),
        }
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
        assert_eq!(source.query["Name"], parsed.query["Name"]);
        assert_eq!(parsed.query["Fields"][0]["Name"], "name");
        assert_eq!(parsed.query["Fields"][1]["Name"], "email");
    }

    #[test]
    fn query_select_json_matches_go() {
        let query = serde_json::json!({
            "Name": "Users",
            "Fields": [{"Name": "name", "Alias": "fullName"}]
        });

        assert_eq!(
            String::from_utf8(query_select_json_bytes(&query).unwrap()).unwrap(),
            r#"{"Name":"Users","Alias":null,"Fields":[{"Name":"name","Alias":"fullName"}],"Limit":null,"Offset":null,"OrderBy":null,"Filter":null,"DocIDs":null,"CIDs":null,"GroupBy":null,"ShowDeleted":false,"IsEncrypted":false}"#
        );
    }
}
