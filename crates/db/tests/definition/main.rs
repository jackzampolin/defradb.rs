//! Schema loading, patching, lens migration and index format.
#[path = "../common/mod.rs"]
mod common;

mod format;
mod helpers;
mod json;
mod loader;
mod migration_suite;
mod patch;
