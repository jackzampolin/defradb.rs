//! The read path: the fetchers a query resolves documents through.

pub mod autocommit;
pub mod commits;
pub mod doc;
pub mod lensed;
pub mod seek;
pub(crate) mod vector;
pub mod versioned;
