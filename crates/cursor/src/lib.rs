//! Opaque cursor token codec for GraphQL cursor pagination.
//!
//! Tokens are `base64url(json{d, k})` — `d` is the document ID,
//! `k` is an alphabetically-ordered map of indexed field values
//! used for index-backed seeking.

mod errors;

pub use errors::CursorError;
