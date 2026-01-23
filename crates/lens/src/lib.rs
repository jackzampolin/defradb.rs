//! Lens schema migration support for DefraDB.
//!
//! Lens enables non-destructive schema evolution via WASM transforms.
//! Documents are transformed at query time from old schema versions to new ones.

mod config;
mod doc;
mod error;
mod history;
mod pipeline;
mod store;
mod wasm;

pub use config::{LensConfig, LensModule};
pub use doc::{LensDoc, DELETED_FIELD, DOC_ID_FIELD};
pub use error::{Error, Result};
pub use history::{build_targeted_history, CollectionHistoryLink, TargetedHistoryLink};
pub use pipeline::{Lens, LensInput};
pub use store::{MemoryTransformStore, TransformId, TransformStore};
pub use wasm::WasmTransformStore;
