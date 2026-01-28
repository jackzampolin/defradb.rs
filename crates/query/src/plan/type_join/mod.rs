//! Type join nodes for resolving relations
//!
//! TypeJoinOne and TypeJoinMany implement the join logic for one-to-one
//! and one-to-many relations respectively. These nodes wrap a parent plan
//! and perform lookups to resolve related documents.

mod direction;
mod join_side;
mod type_join_many;
mod type_join_one;

pub use direction::JoinDirection;
pub use join_side::JoinSide;
pub use type_join_many::{compare_json_values, TypeJoinMany};
pub use type_join_one::{RelationFilter, TypeJoinOne};

// Tests extracted to crates/query/tests/type_join_tests.rs
