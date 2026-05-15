//! Dynamic GraphQL schema generation from CollectionVersion

mod cursor;
mod generator;

pub use cursor::{gen_cursor_collection_field, gen_cursor_query_type, gen_page_info_type};
pub use generator::{
    field_kind_to_gql_type, generate_mutation_type, generate_query_type, generate_schema,
    scalar_to_gql_type, GeneratedSchema, GqlArg, GqlField, GqlInputType, GqlObjectType, GqlType,
};
