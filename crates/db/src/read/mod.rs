//! The read path: the fetchers a query resolves documents through.
pub(crate) mod autocommit;
pub(crate) mod commits;
pub(crate) mod doc;
pub(crate) mod lensed;
pub(crate) mod seek;
pub(crate) mod vector;
pub(crate) mod versioned;
