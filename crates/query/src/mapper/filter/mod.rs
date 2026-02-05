//! Filter types and evaluation
//!
//! This module handles filter parsing, manipulation, and evaluation.
//!
//! # Module Organization (planned)
//!
//! - `filter_impl`: Main Filter struct and all current functionality
//! - `op`: FilterOp enum (placeholder for extraction)
//! - `inspection`: has_*, is_* methods (placeholder for extraction)
//! - `relation`: relation_field_names, extract_relation_filter (placeholder)
//! - `split`: split_by_relation, split_alias (placeholder)
//! - `eval/`: Evaluation logic (placeholder for extraction)
//! - `json_match`: matches_json_object (placeholder)

mod filter_impl;

// Placeholder modules for future extraction
mod eval;
mod inspection;
mod json_match;
mod op;
mod relation;
mod split;

// Re-export everything from filter_impl for backwards compatibility
pub use filter_impl::*;
