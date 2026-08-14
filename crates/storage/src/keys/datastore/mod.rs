/// Datastore keys for document and collection data
///
/// These keys are prefixed with 'd' at the store level and handle:
/// - Document field values
/// - Primary key mappings
/// - Secondary indexes
/// - Search engine artifacts
/// - View caching
mod data_store_key;
mod index_key;
mod misc;
mod vector_index_key;

#[cfg(test)]
mod tests;

pub use data_store_key::*;
pub use index_key::*;
pub use misc::*;
pub use vector_index_key::*;

/// Special field ID for storing document schema version.
///
/// Documents store their schema version ID as a field with this ID.
/// This allows the lens migration system to determine if a document
/// needs to be transformed to match the current collection schema.
///
/// Storage key format: `/{collectionShortID}/v/{docID}/v`
///
/// Matches Go's `keys.DATASTORE_DOC_VERSION_FIELD_ID`.
pub const DATASTORE_DOC_VERSION_FIELD_ID: &str = "v";
