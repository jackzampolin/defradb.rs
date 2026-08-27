//! The SIFT-small corpus: real vectors with published ground truth.
//!
//! Every other corpus here is synthetic, which means recall is measured against
//! an oracle over vectors this repo generated. SIFT ships the true 100 nearest
//! neighbours of each query, so recall against it is a measurement rather than
//! a self-comparison.
//!
//! **It is not embeddings.** These are 128-dimension SIFT image descriptors, so
//! a figure here is a real ANN benchmark number and comparable with published
//! HNSW and FAISS results; it is not a prediction of behaviour on text
//! embeddings.
//!
//! Fetch with `just setup-sift`. Absent, everything here returns `None` and the
//! benchmarks skip, so a fresh clone still runs.

use std::fs;
use std::path::{Path, PathBuf};

pub struct SiftSmall {
    pub base: Vec<Vec<f32>>,
    pub queries: Vec<Vec<f32>>,
    /// `groundtruth[q]` is the true nearest base indices for query `q`,
    /// nearest first.
    pub groundtruth: Vec<Vec<u32>>,
}

impl SiftSmall {
    pub fn load() -> Option<Self> {
        let root = root()?;
        Some(Self {
            base: read_fvecs(&root.join("siftsmall_base.fvecs"))?,
            queries: read_fvecs(&root.join("siftsmall_query.fvecs"))?,
            groundtruth: read_ivecs(&root.join("siftsmall_groundtruth.ivecs"))?,
        })
    }

    pub fn dimensions(&self) -> usize {
        self.base.first().map_or(0, Vec::len)
    }
}

/// `.tooling/sift/siftsmall`, resolved from this package rather than the
/// working directory so it does not matter where cargo was invoked.
fn root() -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()?
        .join(".tooling/sift/siftsmall");
    path.is_dir().then_some(path)
}

/// Each record is a little-endian `u32` dimension followed by that many `f32`s.
fn read_fvecs(path: &Path) -> Option<Vec<Vec<f32>>> {
    let bytes = fs::read(path).ok()?;
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + 4 <= bytes.len() {
        let dimensions = u32::from_le_bytes(bytes[at..at + 4].try_into().ok()?) as usize;
        at += 4;
        let end = at + dimensions * 4;
        if end > bytes.len() {
            return None;
        }
        out.push(
            bytes[at..end]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        );
        at = end;
    }
    Some(out)
}

fn read_ivecs(path: &Path) -> Option<Vec<Vec<u32>>> {
    let bytes = fs::read(path).ok()?;
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + 4 <= bytes.len() {
        let count = u32::from_le_bytes(bytes[at..at + 4].try_into().ok()?) as usize;
        at += 4;
        let end = at + count * 4;
        if end > bytes.len() {
            return None;
        }
        out.push(
            bytes[at..end]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        );
        at = end;
    }
    Some(out)
}

/// Printed once when the corpus is missing, so a skipped benchmark says why.
pub fn skip_notice(what: &str) {
    eprintln!("{what}: skipped, SIFT-small is not present. Run `just setup-sift`.");
}
