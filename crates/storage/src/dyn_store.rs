//! Type-erased [`Store`] wrapper.
//!
//! Erasing the concrete backend at construction time collapses the per-backend
//! monomorphization fan-out (DB, blockstore, P2P sync stack) into a single
//! `S = DynStore` copy-set. Hot paths already dispatch through `Box<dyn Txn>`,
//! so the erasure costs one vtable hop on `new_txn`/`close` only.

use std::sync::Arc;

use async_trait::async_trait;

use crate::corekv::{private, Result, Store, Txn};

/// A [`Store`] that erases the concrete backend behind `Arc<dyn Store>`.
#[derive(Clone)]
pub struct DynStore {
    inner: Arc<dyn Store>,
}

impl DynStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self { inner }
    }
}

impl private::Sealed for DynStore {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Store for DynStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.inner.new_txn(readonly).await
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}
