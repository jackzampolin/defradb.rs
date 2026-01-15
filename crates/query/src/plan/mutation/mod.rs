//! Mutation nodes for write operations

mod create;
mod delete;
mod update;

pub use create::{json_to_normal_value, normal_value_to_json, CreateInput, CreateNode};
pub use delete::DeleteNode;
pub use update::{UpdateInput, UpdateNode};
