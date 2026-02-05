//! Filter evaluation logic
//!
//! This module contains the core evaluation functionality for filters:
//! - `operators`: Comparison, equality, pattern matching, array operators

mod operators;

pub use operators::{eval_op, values_equal};
