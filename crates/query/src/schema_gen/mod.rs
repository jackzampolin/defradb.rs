//! Dynamic GraphQL schema generation from CollectionVersion

mod generator;

pub use generator::{
    generate_mutation_type, generate_query_type, generate_schema, field_kind_to_gql_type,
    scalar_to_gql_type, GeneratedSchema, GqlField, GqlInputType, GqlObjectType, GqlType,
};
