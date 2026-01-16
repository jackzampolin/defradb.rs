//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::{JoinSide, LimitNode, ScanNode, SelectNode, TypeJoinMany, TypeJoinOne};
use crate::planner::PlanNode;

/// Query planner that builds execution plans from Select operations.
pub struct Planner {
    /// Available collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
}

impl Planner {
    /// Create a new planner with the given collection schemas.
    pub fn new(collections: Vec<CollectionVersion>) -> Self {
        let collections = collections
            .into_iter()
            .map(|c| (c.name.clone(), Arc::new(c)))
            .collect();
        Self { collections }
    }

    /// Build an execution plan from a Select operation.
    pub fn plan(&self, select: &Select) -> Result<Box<dyn PlanNode>> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?
            .clone();

        // Build the document mapping for this query
        let mapping = self.build_mapping(select, &collection)?;

        // Build the plan tree bottom-up:
        // ScanNode -> SelectNode -> JoinNodes -> LimitNode

        // 1. ScanNode - reads from storage
        let mut plan: Box<dyn PlanNode> = Box::new(
            ScanNode::new((*collection).clone(), mapping.clone())
                .with_show_deleted(select.show_deleted),
        );

        // 2. Apply filter if present
        if select.filter.is_some() || !select.fields.is_empty() {
            let mut select_node = SelectNode::new(plan, mapping.clone());
            if let Some(ref filter) = select.filter {
                select_node = select_node.with_filter(filter.clone());
            }
            plan = Box::new(select_node);
        }

        // 3. Apply join nodes for relation fields
        plan = self.apply_joins(plan, select, &collection, mapping.clone())?;

        // 4. Apply limit/offset if present
        if let Some(ref limit) = select.limit {
            plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
        }

        Ok(plan)
    }

    /// Apply join nodes for nested selects (relation fields)
    fn apply_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                let relation_field_name = &nested_select.field.name;

                // Find the relation field in the parent collection
                let relation_field = parent_collection
                    .field_by_name(relation_field_name)
                    .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

                // Verify it's a relation field
                if !relation_field.kind.is_relation() {
                    return Err(QueryError::execution(format!(
                        "field '{}' is not a relation",
                        relation_field_name
                    )));
                }

                // Get the target collection
                let target_collection_name = relation_field
                    .kind
                    .relation_collection_id()
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "relation field '{}' has no target collection",
                            relation_field_name
                        ))
                    })?;

                let target_collection = self
                    .collections
                    .get(target_collection_name)
                    .ok_or_else(|| QueryError::collection_not_found(target_collection_name))?
                    .clone();

                // Build the child mapping for the nested select
                let child_mapping = self.build_mapping(nested_select, &target_collection)?;

                // Get the relation field index in the parent mapping
                let relation_field_index = mapping
                    .first_index_of_name(relation_field_name)
                    .ok_or_else(|| QueryError::internal("relation field not in mapping"))?;

                // Set up child mapping in parent
                mapping.set_child_at(relation_field_index, child_mapping.clone());

                // Create the child scan plan
                let child_scan = ScanNode::new((*target_collection).clone(), child_mapping.clone());

                // Find the other side of the relation
                let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                    target_collection.field_by_relation(
                        rel_name,
                        &parent_collection.name,
                        relation_field_name,
                    )
                } else {
                    None
                };

                // Get child relation field index and field
                // For one-to-many and inverted one-to-one joins, we need the child's FK field
                let (child_relation_index, child_relation_field) = if let Some(f) =
                    target_relation_field
                {
                    let idx = target_collection
                        .fields
                        .iter()
                        .position(|tf| tf.name == f.name)
                        .ok_or_else(|| {
                            QueryError::internal(format!(
                                "relation field '{}' not found in collection '{}'",
                                f.name, target_collection.name
                            ))
                        })?;
                    (idx, f.clone())
                } else {
                    // No target relation field found - this can happen for:
                    // 1. Unidirectional relations (no inverse defined)
                    // 2. Self-referential relations without inverse
                    //
                    // For one-to-many joins (parent has array), we MUST find the FK field
                    // on the child side. Look for a field with matching relation_name that
                    // is the primary (FK-holding) side.
                    if relation_field.kind.is_array() {
                        // One-to-many: child must have FK field
                        let child_field = if let Some(rel_name) = &relation_field.relation_name {
                            // Find the primary (non-array) relation field in target collection
                            target_collection
                                .fields
                                .iter()
                                .find(|f| {
                                    f.relation_name.as_deref() == Some(rel_name)
                                        && f.kind.is_relation()
                                        && !f.kind.is_array()
                                })
                        } else {
                            None
                        };

                        match child_field {
                            Some(f) => {
                                let idx = target_collection
                                    .fields
                                    .iter()
                                    .position(|tf| tf.name == f.name)
                                    .unwrap(); // Safe: we just found it above
                                (idx, f.clone())
                            }
                            None => {
                                return Err(QueryError::internal(format!(
                                    "cannot resolve FK field for one-to-many relation '{}' \
                                     on collection '{}': no matching relation field found \
                                     in target collection '{}'",
                                    relation_field_name,
                                    parent_collection.name,
                                    target_collection.name
                                )));
                            }
                        }
                    } else {
                        // One-to-one from secondary side (inverted): need child's FK
                        // The parent doesn't have FK, so child must have it
                        let parent_has_fk = {
                            let id_field_name =
                                CollectionVersion::relation_id_field_name(relation_field_name);
                            parent_collection
                                .fields
                                .iter()
                                .any(|f| f.name == id_field_name)
                        };

                        if !parent_has_fk {
                            // Inverted join - child must have FK
                            let child_field =
                                if let Some(rel_name) = &relation_field.relation_name {
                                    target_collection
                                        .fields
                                        .iter()
                                        .find(|f| {
                                            f.relation_name.as_deref() == Some(rel_name)
                                                && f.kind.is_relation()
                                                && !f.kind.is_array()
                                        })
                                } else {
                                    None
                                };

                            match child_field {
                                Some(f) => {
                                    let idx = target_collection
                                        .fields
                                        .iter()
                                        .position(|tf| tf.name == f.name)
                                        .unwrap();
                                    (idx, f.clone())
                                }
                                None => {
                                    return Err(QueryError::internal(format!(
                                        "cannot resolve FK field for inverted one-to-one relation \
                                         '{}' on collection '{}': no matching relation field \
                                         found in target collection '{}'",
                                        relation_field_name,
                                        parent_collection.name,
                                        target_collection.name
                                    )));
                                }
                            }
                        } else {
                            // Primary side - use index 0 as placeholder (won't be used for FK lookup)
                            (0, relation_field.clone())
                        }
                    }
                };

                // Create join sides
                let parent_side = JoinSide::new(
                    parent_collection.clone(),
                    relation_field.clone(),
                    relation_field_index,
                )
                .as_parent();

                let child_side = JoinSide::new(
                    (*target_collection).clone(),
                    child_relation_field,
                    child_relation_index,
                );

                // Create the appropriate join node
                if relation_field.kind.is_array() {
                    // One-to-many: TypeJoinMany
                    plan = Box::new(TypeJoinMany::new(
                        plan,
                        Box::new(child_scan),
                        parent_side,
                        child_side,
                        mapping.clone(),
                    ));
                } else {
                    // One-to-one: TypeJoinOne
                    plan = Box::new(TypeJoinOne::new(
                        plan,
                        Box::new(child_scan),
                        parent_side,
                        child_side,
                        mapping.clone(),
                    ));
                }
            }
        }

        Ok(plan)
    }

    /// Build the document mapping for a Select operation.
    fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add all requested fields
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    // Validate field exists in schema (skip _docID which is always valid)
                    if field.name != "_docID" && collection.field_by_name(&field.name).is_none() {
                        return Err(QueryError::unknown_field(&field.name));
                    }
                    let index = mapping.next_index();

                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    // Nested select (relation) - add the field but don't recurse here
                    // Child mapping will be built when applying joins
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    return Err(QueryError::execution(format!(
                        "aggregate '{:?}' not yet implemented",
                        agg.aggregate_type
                    )));
                }
            }
        }

        // If no fields specified, add all collection fields
        if mapping.next_index() == 0 {
            for (i, field) in collection.fields.iter().enumerate() {
                mapping.add(i, &field.name);
                mapping.add_render_key(i, &field.name);
            }
        }

        Ok(mapping)
    }

    /// Get a collection schema by name.
    pub fn collection(&self, name: &str) -> Option<&Arc<CollectionVersion>> {
        self.collections.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapper::{Field, Filter};
    use schema::{FieldDescription, FieldKind};

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    fn make_users_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // One-to-many relation to posts (array)
                FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                    .with_relation_name("author_posts"),
            ],
        )
    }

    fn make_posts_collection() -> CollectionVersion {
        CollectionVersion::new(
            "posts",
            "v1",
            "coll-posts",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "title", FieldKind::string()),
                // Many-to-one relation to users (singular)
                FieldDescription::new("3", "author", FieldKind::relation("users", false))
                    .with_relation_name("author_posts")
                    .as_primary(),
                // Auto-generated FK field
                FieldDescription::new("4", "author_id", FieldKind::doc_id())
                    .with_relation_name("author_posts")
                    .as_primary(),
            ],
        )
    }

    #[test]
    fn test_planner_new() {
        let planner = Planner::new(vec![make_test_collection()]);
        assert!(planner.collection("Users").is_some());
        assert!(planner.collection("Posts").is_none());
    }

    #[tokio::test]
    async fn test_plan_simple_select() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_with_limit() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_limit(10);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "limitNode");
    }

    #[tokio::test]
    async fn test_plan_unknown_collection() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Posts").with_field(Field::new("title"));

        let result = planner.plan(&select);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_with_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[test]
    fn test_build_mapping() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("_docID"));
        assert!(mapping.has_field("name"));
        assert!(!mapping.has_field("age"));
    }

    #[test]
    fn test_build_mapping_with_alias() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users").with_field(Field::with_alias("name", "userName"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("name"));
        // Should have render key "userName"
        assert_eq!(mapping.render_keys.len(), 1);
        assert_eq!(mapping.render_keys[0].key, "userName");
    }

    // ========================================================================
    // Join Planning Tests
    // ========================================================================

    #[tokio::test]
    async fn test_plan_with_one_to_one_relation() {
        // Query: posts { title, author { name } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select for author - field name is "author" (relation field), collection is "users"
        let author_select = Select::new("users")
            .with_field_name("author")
            .with_field(Field::new("name"));

        let select = Select::new("posts")
            .with_field(Field::new("title"))
            .with_select(author_select);

        let plan = planner.plan(&select).unwrap();

        // The plan should be a TypeJoinOne (for one-to-one)
        assert_eq!(plan.kind(), "typeJoinOne");
    }

    #[tokio::test]
    async fn test_plan_with_one_to_many_relation() {
        // Query: users { name, posts { title } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select for posts - field name is "posts" (relation field), collection is "posts"
        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"));

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select);

        let plan = planner.plan(&select).unwrap();

        // The plan should be a TypeJoinMany (for one-to-many)
        assert_eq!(plan.kind(), "typeJoinMany");
    }

    #[tokio::test]
    async fn test_plan_relation_unknown_field() {
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Try to select a non-existent relation field
        let nested = Select::new("users")
            .with_field_name("nonexistent")
            .with_field(Field::new("name"));

        let select = Select::new("posts")
            .with_field(Field::new("title"))
            .with_select(nested);

        let result = planner.plan(&select);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_relation_with_limit() {
        // Query: users { name, posts { title } } limit 5
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"));

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select)
            .with_limit(5);

        let plan = planner.plan(&select).unwrap();

        // The outermost node should be a LimitNode wrapping the join
        assert_eq!(plan.kind(), "limitNode");

        // The source should be the join
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeJoinMany");
    }
}
