//! Dynamic GraphQL schema generation from CollectionVersion

mod generator;

pub use generator::{
    field_kind_to_gql_type, generate_mutation_type, generate_query_type, generate_schema,
    scalar_to_gql_type, GeneratedSchema, GqlField, GqlInputType, GqlObjectType, GqlType,
};
