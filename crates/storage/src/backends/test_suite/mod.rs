/// Shared test suite for all backend implementations.
///
/// This module provides a comprehensive test suite that verifies backend correctness.
/// All backends MUST pass these tests to ensure consistent behavior.
///
/// # Usage
///
/// Each backend module should include:
/// ```ignore
/// #[cfg(test)]
/// mod shared_tests {
///     use super::*;
///     use crate::backends::test_suite::*;
///
///     async fn create_store() -> impl Store {
///         MemoryStore::new()
///     }
///
///     // Then invoke the test macros or call test functions
/// }
/// ```
mod basic_ops;
mod callbacks;
mod concurrency;
mod dropable;
mod iterators;
mod macros;

pub use basic_ops::*;
pub use callbacks::*;
pub use concurrency::*;
pub use dropable::*;
pub use iterators::*;
pub use macros::*;
