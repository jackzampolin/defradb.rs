//! Dispatch over the configured algorithm.
//!
//! An enum rather than `Box<dyn VectorIndexEngine>`: the trait is generic over
//! the element width and the admission predicate, so it is not object-safe and
//! making it so would cost a monomorphised kernel per call site.

use super::ann::{Admit, EngineKind, Neighbor, VectorIndexEngine};
use super::flat::Flat;
use super::hnsw::Hnsw;
use crate::error::Result;
use crate::vector::core::Element;
use crate::vector::store::{NodeId, VectorNodeStore};

#[derive(Debug)]
pub enum Engine<S> {
    Hnsw(Hnsw<S>),
    Flat(Flat<S>),
}

macro_rules! dispatch {
    ($self:ident, $engine:ident => $call:expr) => {
        match $self {
            Engine::Hnsw($engine) => $call,
            Engine::Flat($engine) => $call,
        }
    };
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for Engine<S> {
    fn kind(&self) -> EngineKind {
        dispatch!(self, e => e.kind())
    }

    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        dispatch!(self, e => e.insert(id, vector).await)
    }

    async fn delete(&mut self, id: NodeId) -> Result<bool> {
        dispatch!(self, e => e.delete(id).await)
    }

    async fn search_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        dispatch!(self, e => e.search_where(query, k, effort, admit).await)
    }
}
