//! Dynamic GraphQL schema generation from CollectionVersion

use query_model::error::{QueryError, Result};
use schema::{CollectionVersion, FieldKind, ScalarKind};

/// GraphQL type representation
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GqlType {
    /// Non-null type
    NonNull(Box<GqlType>),
    /// List type
    List(Box<GqlType>),
    /// Named type (e.g., "String", "Int", "User")
    Named(String),
}

impl GqlType {
    pub fn string() -> Self {
        GqlType::Named("String".to_string())
    }

    pub fn int() -> Self {
        GqlType::Named("Int".to_string())
    }

    pub fn float() -> Self {
        GqlType::Named("Float".to_string())
    }

    pub fn boolean() -> Self {
        GqlType::Named("Boolean".to_string())
    }

    pub fn id() -> Self {
        GqlType::Named("ID".to_string())
    }

    pub fn datetime() -> Self {
        GqlType::Named("DateTime".to_string())
    }

    pub fn json() -> Self {
        GqlType::Named("JSON".to_string())
    }

    pub fn blob() -> Self {
        GqlType::Named("Blob".to_string())
    }

    pub fn named(name: impl Into<String>) -> Self {
        GqlType::Named(name.into())
    }

    pub fn list(inner: GqlType) -> Self {
        GqlType::List(Box::new(inner))
    }

    pub fn non_null(inner: GqlType) -> Self {
        GqlType::NonNull(Box::new(inner))
    }

    /// Convert to GraphQL SDL string representation
    pub fn to_sdl(&self) -> String {
        match self {
            GqlType::NonNull(inner) => format!("{}!", inner.to_sdl()),
            GqlType::List(inner) => format!("[{}]", inner.to_sdl()),
            GqlType::Named(name) => name.clone(),
        }
    }
}

/// A GraphQL field definition
#[derive(Debug, Clone)]
pub struct GqlField {
    pub name: String,
    pub field_type: GqlType,
    pub description: Option<String>,
}

impl GqlField {
    pub fn new(name: impl Into<String>, field_type: GqlType) -> Self {
        Self {
            name: name.into(),
            field_type,
            description: None,
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Convert to GraphQL SDL string
    pub fn to_sdl(&self) -> String {
        let desc = self
            .description
            .as_ref()
            .map(|d| format!("  \"{}\"\n", d))
            .unwrap_or_default();
        format!("{}  {}: {}", desc, self.name, self.field_type.to_sdl())
    }
}

/// A GraphQL type definition (object type)
#[derive(Debug, Clone)]
pub struct GqlObjectType {
    pub name: String,
    pub fields: Vec<GqlField>,
    pub description: Option<String>,
}

impl GqlObjectType {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
            description: None,
        }
    }

    pub fn with_field(mut self, field: GqlField) -> Self {
        self.fields.push(field);
        self
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Convert to GraphQL SDL string
    pub fn to_sdl(&self) -> String {
        let desc = self
            .description
            .as_ref()
            .map(|d| format!("\"\"\"\n{}\n\"\"\"\n", d))
            .unwrap_or_default();
        let fields: Vec<String> = self.fields.iter().map(|f| f.to_sdl()).collect();
        format!("{}type {} {{\n{}\n}}", desc, self.name, fields.join("\n"))
    }
}

/// A GraphQL input type definition
#[derive(Debug, Clone)]
pub struct GqlInputType {
    pub name: String,
    pub fields: Vec<GqlField>,
}

impl GqlInputType {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
        }
    }

    pub fn with_field(mut self, field: GqlField) -> Self {
        self.fields.push(field);
        self
    }

    /// Convert to GraphQL SDL string
    pub fn to_sdl(&self) -> String {
        let fields: Vec<String> = self.fields.iter().map(|f| f.to_sdl()).collect();
        format!("input {} {{\n{}\n}}", self.name, fields.join("\n"))
    }
}

/// Generated GraphQL schema for a collection
#[derive(Debug, Clone)]
pub struct GeneratedSchema {
    /// The main object type (e.g., "User")
    pub object_type: GqlObjectType,
    /// Input type for create mutations (e.g., "CreateUserInput")
    pub create_input: GqlInputType,
    /// Input type for update mutations (e.g., "UpdateUserInput")
    pub update_input: GqlInputType,
    /// Filter input type (e.g., "UserFilterInput")
    pub filter_input: GqlInputType,
    /// Order input type (e.g., "UserOrderInput")
    pub order_input: GqlInputType,
}

/// Convert a FieldKind to a GraphQL type
pub fn field_kind_to_gql_type(
    kind: &FieldKind,
    collections: &[&CollectionVersion],
) -> Result<GqlType> {
    match kind {
        FieldKind::Scalar(scalar) => Ok(scalar_to_gql_type(scalar)),
        FieldKind::ScalarArray(array) => {
            let element_type = scalar_to_gql_type(&array.element_kind());
            if array.has_nillable_elements() {
                Ok(GqlType::list(element_type))
            } else {
                Ok(GqlType::list(GqlType::non_null(element_type)))
            }
        }
        FieldKind::Relation {
            collection_id,
            is_array,
        } => {
            // Find the collection name from the ID
            let type_name = collections
                .iter()
                .find(|c| &c.collection_id == collection_id)
                .map(|c| c.name.clone())
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation references unknown collection: {}",
                        collection_id
                    ))
                })?;

            if *is_array {
                Ok(GqlType::list(GqlType::named(type_name)))
            } else {
                Ok(GqlType::named(type_name))
            }
        }
        FieldKind::SelfRef { is_array, .. } => {
            // Self references use the current type name
            // This should be resolved by the caller
            if *is_array {
                Ok(GqlType::list(GqlType::named("Self")))
            } else {
                Ok(GqlType::named("Self"))
            }
        }
        FieldKind::Named { name, is_array } => {
            if *is_array {
                Ok(GqlType::list(GqlType::named(name.clone())))
            } else {
                Ok(GqlType::named(name.clone()))
            }
        }
        _ => Ok(GqlType::string()),
    }
}

/// Convert a ScalarKind to a GraphQL type
pub fn scalar_to_gql_type(scalar: &ScalarKind) -> GqlType {
    match scalar {
        ScalarKind::None => GqlType::named("Void"),
        ScalarKind::DocID => GqlType::id(),
        ScalarKind::Bool => GqlType::boolean(),
        ScalarKind::Int => GqlType::int(),
        ScalarKind::Float64 | ScalarKind::Float32 => GqlType::float(),
        ScalarKind::DateTime => GqlType::datetime(),
        ScalarKind::String => GqlType::string(),
        ScalarKind::Blob => GqlType::blob(),
        ScalarKind::Json => GqlType::json(),
        _ => GqlType::string(),
    }
}

/// Generate a complete GraphQL schema from a collection
pub fn generate_schema(
    collection: &CollectionVersion,
    all_collections: &[&CollectionVersion],
) -> Result<GeneratedSchema> {
    let mut object_type = GqlObjectType::new(&collection.name)
        .with_description(format!("{} collection type", collection.name));

    let mut create_input = GqlInputType::new(format!("Create{}Input", collection.name));
    let mut update_input = GqlInputType::new(format!("Update{}Input", collection.name));
    let mut filter_input = GqlInputType::new(format!("{}FilterInput", collection.name));
    let order_input = GqlInputType::new(format!("{}OrderInput", collection.name));

    // Add _docID field to object type
    object_type = object_type.with_field(GqlField::new("_docID", GqlType::non_null(GqlType::id())));

    // Add _deleted field to object type (soft-delete status)
    object_type = object_type.with_field(GqlField::new(
        "_deleted",
        GqlType::non_null(GqlType::boolean()),
    ));

    // Process each field
    for field in &collection.fields {
        // Skip internal fields
        if field.name.starts_with('_') && field.name != "_docID" {
            continue;
        }

        let gql_type = field_kind_to_gql_type(&field.kind, all_collections)?;

        // Add to object type
        object_type = object_type.with_field(GqlField::new(&field.name, gql_type.clone()));

        // Add to create input (skip _docID as it's auto-generated)
        if field.name != "_docID" {
            create_input = create_input.with_field(GqlField::new(&field.name, gql_type.clone()));
            update_input = update_input.with_field(GqlField::new(&field.name, gql_type.clone()));
        }

        // Add to filter input (only scalars)
        if field.kind.is_scalar() {
            let filter_type =
                GqlType::named(format!("{}Filter", scalar_filter_type_name(&field.kind)));
            filter_input = filter_input.with_field(GqlField::new(&field.name, filter_type));
        }
    }

    Ok(GeneratedSchema {
        object_type,
        create_input,
        update_input,
        filter_input,
        order_input,
    })
}

fn scalar_filter_type_name(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Scalar(ScalarKind::Bool) => "Boolean",
        FieldKind::Scalar(ScalarKind::Int) => "Int",
        FieldKind::Scalar(ScalarKind::Float64 | ScalarKind::Float32) => "Float",
        FieldKind::Scalar(ScalarKind::String) => "String",
        FieldKind::Scalar(ScalarKind::DateTime) => "DateTime",
        FieldKind::Scalar(ScalarKind::DocID) => "ID",
        _ => "Any",
    }
}

/// Generate Query type with all collection queries
pub fn generate_query_type(collections: &[&CollectionVersion]) -> GqlObjectType {
    let mut query = GqlObjectType::new("Query");

    for collection in collections {
        let type_name = &collection.name;

        // List query (e.g., users(filter: UserFilterInput, limit: Int, offset: Int): [User!]!)
        query = query.with_field(GqlField::new(
            type_name.to_lowercase(),
            GqlType::non_null(GqlType::list(GqlType::non_null(GqlType::named(type_name)))),
        ));
    }

    query
}

/// Generate Mutation type with all collection mutations
pub fn generate_mutation_type(collections: &[&CollectionVersion]) -> GqlObjectType {
    let mut mutation = GqlObjectType::new("Mutation");

    for collection in collections {
        let type_name = &collection.name;

        // Add mutation (create document)
        mutation = mutation.with_field(GqlField::new(
            format!("add_{}", type_name),
            GqlType::list(GqlType::named(type_name)),
        ));

        // Update mutation
        mutation = mutation.with_field(GqlField::new(
            format!("update_{}", type_name),
            GqlType::list(GqlType::named(type_name)),
        ));

        // Delete mutation
        mutation = mutation.with_field(GqlField::new(
            format!("delete_{}", type_name),
            GqlType::list(GqlType::named(type_name)),
        ));

        // Upsert mutation (Go syntax: filter, create, update)
        mutation = mutation.with_field(GqlField::new(
            format!("upsert_{}", type_name),
            GqlType::list(GqlType::named(type_name)),
        ));
    }

    mutation
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::FieldDescription;

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "User",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
                FieldDescription::new("4", "active", FieldKind::bool()),
                FieldDescription::new("5", "tags", FieldKind::string_array()),
            ],
        )
    }

    #[test]
    fn test_gql_type_sdl() {
        assert_eq!(GqlType::string().to_sdl(), "String");
        assert_eq!(GqlType::non_null(GqlType::string()).to_sdl(), "String!");
        assert_eq!(GqlType::list(GqlType::string()).to_sdl(), "[String]");
        assert_eq!(
            GqlType::non_null(GqlType::list(GqlType::non_null(GqlType::string()))).to_sdl(),
            "[String!]!"
        );
    }

    #[test]
    fn test_scalar_to_gql_type() {
        assert_eq!(scalar_to_gql_type(&ScalarKind::String), GqlType::string());
        assert_eq!(scalar_to_gql_type(&ScalarKind::Int), GqlType::int());
        assert_eq!(scalar_to_gql_type(&ScalarKind::Bool), GqlType::boolean());
        assert_eq!(scalar_to_gql_type(&ScalarKind::Float64), GqlType::float());
        assert_eq!(scalar_to_gql_type(&ScalarKind::DocID), GqlType::id());
    }

    #[test]
    fn test_generate_schema() {
        let collection = make_test_collection();
        let collections: Vec<&CollectionVersion> = vec![&collection];

        let schema = generate_schema(&collection, &collections).unwrap();

        assert_eq!(schema.object_type.name, "User");
        assert!(!schema.object_type.fields.is_empty());

        // Check that _docID is present
        let doc_id_field = schema
            .object_type
            .fields
            .iter()
            .find(|f| f.name == "_docID");
        assert!(doc_id_field.is_some());

        // Check that name field is present
        let name_field = schema.object_type.fields.iter().find(|f| f.name == "name");
        assert!(name_field.is_some());
    }

    #[test]
    fn test_object_type_sdl() {
        let collection = make_test_collection();
        let collections: Vec<&CollectionVersion> = vec![&collection];

        let schema = generate_schema(&collection, &collections).unwrap();
        let sdl = schema.object_type.to_sdl();

        assert!(sdl.contains("type User"));
        assert!(sdl.contains("_docID: ID!"));
        assert!(sdl.contains("name: String"));
        assert!(sdl.contains("age: Int"));
        assert!(sdl.contains("active: Boolean"));
    }

    #[test]
    fn test_generate_query_type() {
        let collection = make_test_collection();
        let collections: Vec<&CollectionVersion> = vec![&collection];

        let query = generate_query_type(&collections);

        assert_eq!(query.name, "Query");
        let user_query = query.fields.iter().find(|f| f.name == "user");
        assert!(user_query.is_some());
    }

    #[test]
    fn test_generate_mutation_type() {
        let collection = make_test_collection();
        let collections: Vec<&CollectionVersion> = vec![&collection];

        let mutation = generate_mutation_type(&collections);

        assert_eq!(mutation.name, "Mutation");

        let create = mutation.fields.iter().find(|f| f.name == "add_User");
        assert!(create.is_some());

        let update = mutation.fields.iter().find(|f| f.name == "update_User");
        assert!(update.is_some());

        let delete = mutation.fields.iter().find(|f| f.name == "delete_User");
        assert!(delete.is_some());

        let upsert = mutation.fields.iter().find(|f| f.name == "upsert_User");
        assert!(upsert.is_some());
    }

    #[test]
    fn test_field_kind_to_gql_type_relation() {
        let user_collection = CollectionVersion::new(
            "User",
            "v1",
            "coll-users",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        );

        let collections: Vec<&CollectionVersion> = vec![&user_collection];

        // One-to-one relation
        let relation = FieldKind::relation("coll-users", false);
        let gql_type = field_kind_to_gql_type(&relation, &collections).unwrap();
        assert_eq!(gql_type, GqlType::named("User"));

        // One-to-many relation
        let relation_array = FieldKind::relation("coll-users", true);
        let gql_type_array = field_kind_to_gql_type(&relation_array, &collections).unwrap();
        assert_eq!(gql_type_array, GqlType::list(GqlType::named("User")));
    }

    #[test]
    fn test_field_kind_to_gql_type_unknown_collection() {
        let collections: Vec<&CollectionVersion> = vec![];

        // Relation to unknown collection should error
        let relation = FieldKind::relation("unknown-collection", false);
        let result = field_kind_to_gql_type(&relation, &collections);
        assert!(result.is_err());
    }
}
