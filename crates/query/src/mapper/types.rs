//! Core mapper types for query operations

use crate::document::DocumentMapping;
use crate::mapper::filter::Filter;

/// Order direction for sorting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

impl OrderDirection {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ASC" => Some(Self::Asc),
            "DESC" => Some(Self::Desc),
            _ => None,
        }
    }
}

/// A single order condition
#[derive(Debug, Clone)]
pub struct OrderCondition {
    /// Field path for ordering (may be compound for nested objects)
    pub fields: Vec<String>,
    /// Sort direction
    pub direction: OrderDirection,
}

impl OrderCondition {
    pub fn new(field: impl Into<String>, direction: OrderDirection) -> Self {
        Self {
            fields: vec![field.into()],
            direction,
        }
    }

    pub fn with_path(fields: Vec<String>, direction: OrderDirection) -> Self {
        Self { fields, direction }
    }
}

/// Order by specification
#[derive(Debug, Clone, Default)]
pub struct OrderBy {
    pub conditions: Vec<OrderCondition>,
}

impl OrderBy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_condition(mut self, condition: OrderCondition) -> Self {
        self.conditions.push(condition);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Check if any order condition references a relation field (nested path).
    ///
    /// A path like `["author", "age"]` indicates ordering through a relation.
    /// This returns true if any condition has a path length > 1.
    pub fn has_relation_order(&self) -> bool {
        self.conditions.iter().any(|c| c.fields.len() > 1)
    }

    /// Get the names of relation fields referenced in order conditions.
    ///
    /// For a path like `["author", "age"]`, returns `"author"`.
    /// Only returns fields from paths with length > 1.
    pub fn relation_field_names(&self) -> Vec<String> {
        self.conditions
            .iter()
            .filter(|c| c.fields.len() > 1)
            .filter_map(|c| c.fields.first().cloned())
            .collect()
    }
}

/// Limit and offset for pagination
#[derive(Debug, Clone, Default)]
pub struct Limit {
    pub limit: Option<u64>,
    pub offset: u64,
}

impl Limit {
    pub fn new(limit: Option<u64>, offset: u64) -> Self {
        Self { limit, offset }
    }

    pub fn limit_only(limit: u64) -> Self {
        Self {
            limit: Some(limit),
            offset: 0,
        }
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }
}

/// Group by specification
#[derive(Debug, Clone, Default)]
pub struct GroupBy {
    pub fields: Vec<String>,
}

impl GroupBy {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// A simple field reference
#[derive(Debug, Clone)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Optional alias
    pub alias: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: None,
        }
    }

    pub fn with_alias(name: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    /// Get the output name (alias if set, otherwise name)
    pub fn output_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Selection type for polymorphic queries
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionType {
    /// Normal object selection
    Object,
    /// Commit/history selection
    Commit,
    /// Encrypted search selection
    EncryptedSearch,
}

impl Default for SelectionType {
    fn default() -> Self {
        Self::Object
    }
}

/// Requestable items in a select (field, aggregate, or sub-select)
#[derive(Debug, Clone)]
pub enum Requestable {
    /// Simple field
    Field(Field),
    /// Nested select (for relations)
    Select(Box<Select>),
    /// Aggregate function
    Aggregate(Aggregate),
}

/// Aggregate function type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    Count,
    Sum,
    Average,
    Min,
    Max,
}

impl AggregateType {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "_count" => Some(Self::Count),
            "_sum" => Some(Self::Sum),
            "_avg" => Some(Self::Average),
            "_min" => Some(Self::Min),
            "_max" => Some(Self::Max),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Count => "_count",
            Self::Sum => "_sum",
            Self::Average => "_avg",
            Self::Min => "_min",
            Self::Max => "_max",
        }
    }
}

/// An aggregate operation
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Type of aggregate
    pub aggregate_type: AggregateType,
    /// Target field(s) for the aggregate
    pub targets: Vec<AggregateTarget>,
    /// Optional filter for the aggregate
    pub filter: Option<Filter>,
    /// Optional alias for output
    pub alias: Option<String>,
}

impl Aggregate {
    pub fn count() -> Self {
        Self {
            aggregate_type: AggregateType::Count,
            targets: Vec::new(),
            filter: None,
            alias: None,
        }
    }

    pub fn sum(target: AggregateTarget) -> Self {
        Self {
            aggregate_type: AggregateType::Sum,
            targets: vec![target],
            filter: None,
            alias: None,
        }
    }

    pub fn avg(target: AggregateTarget) -> Self {
        Self {
            aggregate_type: AggregateType::Average,
            targets: vec![target],
            filter: None,
            alias: None,
        }
    }

    pub fn min(target: AggregateTarget) -> Self {
        Self {
            aggregate_type: AggregateType::Min,
            targets: vec![target],
            filter: None,
            alias: None,
        }
    }

    pub fn max(target: AggregateTarget) -> Self {
        Self {
            aggregate_type: AggregateType::Max,
            targets: vec![target],
            filter: None,
            alias: None,
        }
    }

    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_target(mut self, target: AggregateTarget) -> Self {
        self.targets.push(target);
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Get the output name (alias if set, otherwise the aggregate type name)
    pub fn output_name(&self) -> &str {
        self.alias
            .as_deref()
            .unwrap_or_else(|| self.aggregate_type.as_str())
    }
}

/// Target for an aggregate function
#[derive(Debug, Clone)]
pub struct AggregateTarget {
    /// Host name (collection or field group)
    pub host_name: String,
    /// Field name to aggregate
    pub field_name: Option<String>,
    /// Optional filter
    pub filter: Option<Filter>,
    /// Limit for the target
    pub limit: Option<Limit>,
    /// Order for the target
    pub order: Option<OrderBy>,
}

impl AggregateTarget {
    pub fn new(host_name: impl Into<String>) -> Self {
        Self {
            host_name: host_name.into(),
            field_name: None,
            filter: None,
            limit: None,
            order: None,
        }
    }

    pub fn with_field(host_name: impl Into<String>, field_name: impl Into<String>) -> Self {
        Self {
            host_name: host_name.into(),
            field_name: Some(field_name.into()),
            filter: None,
            limit: None,
            order: None,
        }
    }
}

/// A complete select operation
#[derive(Debug, Clone)]
pub struct Select {
    /// Collection name
    pub collection_name: String,
    /// Field being selected (for nested selects)
    pub field: Field,
    /// Child fields to return
    pub fields: Vec<Requestable>,
    /// Optional filter
    pub filter: Option<Filter>,
    /// Optional limit/offset
    pub limit: Option<Limit>,
    /// Optional ordering
    pub order_by: Option<OrderBy>,
    /// Optional grouping
    pub group_by: Option<GroupBy>,
    /// Document ID filter
    pub doc_ids: Option<Vec<String>>,
    /// CID filter (for versioned queries)
    pub cid: Option<String>,
    /// Whether to include deleted documents
    pub show_deleted: bool,
    /// Whether this is an encrypted search
    pub is_encrypted: bool,
    /// Selection type
    pub selection_type: SelectionType,
    /// Document mapping for this select
    pub document_mapping: DocumentMapping,
}

impl Select {
    /// Create a new select for a collection
    pub fn new(collection_name: impl Into<String>) -> Self {
        let collection_name = collection_name.into();
        Self {
            field: Field::new(&collection_name),
            collection_name,
            fields: Vec::new(),
            filter: None,
            limit: None,
            order_by: None,
            group_by: None,
            doc_ids: None,
            cid: None,
            show_deleted: false,
            is_encrypted: false,
            selection_type: SelectionType::Object,
            document_mapping: DocumentMapping::new(),
        }
    }

    /// Add a field to select
    pub fn with_field(mut self, field: Field) -> Self {
        self.fields.push(Requestable::Field(field));
        self
    }

    /// Add a nested select
    pub fn with_select(mut self, select: Select) -> Self {
        self.fields.push(Requestable::Select(Box::new(select)));
        self
    }

    /// Set the field name (for nested selects where field name differs from collection name)
    pub fn with_field_name(mut self, name: impl Into<String>) -> Self {
        self.field.name = name.into();
        self
    }

    /// Add an aggregate
    pub fn with_aggregate(mut self, aggregate: Aggregate) -> Self {
        self.fields.push(Requestable::Aggregate(aggregate));
        self
    }

    /// Set filter
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set limit
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(Limit::limit_only(limit));
        self
    }

    /// Set limit and offset
    pub fn with_limit_offset(mut self, limit: u64, offset: u64) -> Self {
        self.limit = Some(Limit::new(Some(limit), offset));
        self
    }

    /// Set order by
    pub fn with_order(mut self, order_by: OrderBy) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Set group by
    pub fn with_group_by(mut self, group_by: GroupBy) -> Self {
        self.group_by = Some(group_by);
        self
    }

    /// Set doc ID filter
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Set CID filter
    pub fn with_cid(mut self, cid: String) -> Self {
        self.cid = Some(cid);
        self
    }

    /// Show deleted documents
    pub fn with_show_deleted(mut self) -> Self {
        self.show_deleted = true;
        self
    }

    /// Get all requested simple fields (not nested selects or aggregates)
    pub fn requested_fields(&self) -> Vec<&Field> {
        self.fields
            .iter()
            .filter_map(|r| match r {
                Requestable::Field(f) => Some(f),
                _ => None,
            })
            .collect()
    }

    /// Create a version of this Select filtered to a specific document ID.
    ///
    /// This is used by subscriptions to efficiently query only the document
    /// that was updated, rather than re-executing the full query.
    ///
    /// If the Select already has doc_ids set, they are preserved and the
    /// new doc_id is added if not already present.
    pub fn to_subscription_select(&self, doc_id: String) -> Self {
        let mut select = self.clone();
        match &mut select.doc_ids {
            Some(ids) => {
                // Add doc_id if not already present
                if !ids.contains(&doc_id) {
                    ids.push(doc_id);
                }
            }
            None => {
                select.doc_ids = Some(vec![doc_id]);
            }
        }
        select
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_direction() {
        assert_eq!(OrderDirection::parse("ASC"), Some(OrderDirection::Asc));
        assert_eq!(OrderDirection::parse("desc"), Some(OrderDirection::Desc));
        assert_eq!(OrderDirection::parse("invalid"), None);
    }

    #[test]
    fn test_field_output_name() {
        let f = Field::new("name");
        assert_eq!(f.output_name(), "name");

        let f = Field::with_alias("name", "userName");
        assert_eq!(f.output_name(), "userName");
    }

    #[test]
    fn test_aggregate_type() {
        assert_eq!(AggregateType::parse("_count"), Some(AggregateType::Count));
        assert_eq!(AggregateType::parse("_avg"), Some(AggregateType::Average));
        assert_eq!(AggregateType::parse("invalid"), None);
    }

    #[test]
    fn test_select_builder() {
        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_field(Field::new("age"))
            .with_limit(10);

        assert_eq!(select.collection_name, "users");
        assert_eq!(select.fields.len(), 2);
        assert!(select.limit.is_some());
        assert_eq!(select.limit.as_ref().unwrap().limit, Some(10));
    }

    #[test]
    fn test_order_by() {
        let order = OrderBy::new()
            .with_condition(OrderCondition::new("name", OrderDirection::Asc))
            .with_condition(OrderCondition::new("age", OrderDirection::Desc));

        assert_eq!(order.conditions.len(), 2);
        assert_eq!(order.conditions[0].direction, OrderDirection::Asc);
        assert_eq!(order.conditions[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_aggregate() {
        let agg = Aggregate::sum(AggregateTarget::with_field("users", "age"));
        assert_eq!(agg.aggregate_type, AggregateType::Sum);
        assert_eq!(agg.targets[0].field_name, Some("age".to_string()));
    }

    #[test]
    fn test_to_subscription_select() {
        // Test adding doc_id when none present
        let select = Select::new("users").with_field(Field::new("name"));

        assert!(select.doc_ids.is_none());

        let filtered = select.to_subscription_select("doc-123".to_string());
        assert_eq!(filtered.doc_ids, Some(vec!["doc-123".to_string()]));
        assert_eq!(filtered.collection_name, "users");
        assert_eq!(filtered.fields.len(), 1);
    }

    #[test]
    fn test_to_subscription_select_with_existing_doc_ids() {
        // Test adding doc_id when some already present
        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_doc_ids(vec!["doc-1".to_string(), "doc-2".to_string()]);

        let filtered = select.to_subscription_select("doc-3".to_string());
        let doc_ids = filtered.doc_ids.unwrap();
        assert_eq!(doc_ids.len(), 3);
        assert!(doc_ids.contains(&"doc-1".to_string()));
        assert!(doc_ids.contains(&"doc-2".to_string()));
        assert!(doc_ids.contains(&"doc-3".to_string()));
    }

    #[test]
    fn test_to_subscription_select_no_duplicate() {
        // Test that existing doc_id is not duplicated
        let select = Select::new("users").with_doc_ids(vec!["doc-123".to_string()]);

        let filtered = select.to_subscription_select("doc-123".to_string());
        let doc_ids = filtered.doc_ids.unwrap();
        assert_eq!(doc_ids.len(), 1);
        assert_eq!(doc_ids[0], "doc-123");
    }
}
