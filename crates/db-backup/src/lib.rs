//! Database export/import (backup) for DefraDB.
//!
//! Extracted from the main `db` crate as part of the #669 decomposition
//! epic. This crate depends on `db` for the `DB<S>` handle but is only
//! used by high-level consumers (CLI, FFI) — the `db` crate itself does
//! not depend on it, so the dependency direction stays one-way.
//!
//! Public API:
//! - [`export_database`] — serialize one or all collections to a JSON string
//! - [`import_database`] — import documents from a JSON string
//! - [`ImportStats`] — statistics returned from an import
//!
//! Internal helpers (`classify_schema_fields`,
//! `json_to_graphql_input`, `FieldInfo`) are also re-exported for callers
//! that need to inspect or reuse the classification logic.

pub mod backup;

pub use backup::{
    classify_schema_fields, export_database, import_database, json_to_graphql_input, FieldInfo,
    ImportStats,
};
