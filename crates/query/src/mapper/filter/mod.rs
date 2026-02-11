//! Filter types and evaluation
//!
//! This module handles filter parsing, manipulation, and evaluation.
//!
//! # Module Organization
//!
//! - `op`: FilterOp enum for filter operators
//! - `filter_impl`: Filter struct, constructors, and core evaluation (matches, eval_conditions)
//! - `inspection`: Boolean queries about filter structure (has_*, is_*)
//! - `relation`: Relation filter extraction and path analysis
//! - `split`: Filter decomposition for query planning
//! - `eval/`: Operator evaluation (comparison, equality, pattern matching)
//! - `json_match`: JSON object/scalar matching for aggregate filters

mod eval_alias;
mod eval_relation;
mod filter_impl;
#[cfg(test)]
mod filter_tests;

mod eval;
mod inspection;
mod json_match;
pub mod op;
mod relation;
mod split;

// Re-export FilterOp from op module
pub use op::FilterOp;

// Re-export everything from filter_impl for backwards compatibility
pub use eval::like_pattern_match;
pub use filter_impl::*;
