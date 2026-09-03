//! IVF_FLAT build and search parameters.

pub use crate::index::vector::engine::ivf::{MAX_NLIST, TRAIN_PER_LIST};

use crate::index::error::{Error, Result};
use crate::index::vector::engine::ivf;

pub const DEFAULT_NPROBE: u32 = 8;
pub const DEFAULT_SAMPLE_BYTES: u64 = 128 << 20;

/// `0` means derive `nlist` from the corpus, matching
/// [`IvfPqParams`](crate::index::vector::engine::ivfpq::IvfPqParams). There is
/// no `m`: a list holds the full vector, so there is nothing to quantize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IvfFlatParams {
    pub nlist: u32,
    pub nprobe: u32,
    pub sample_bytes: u64,
}

impl Default for IvfFlatParams {
    fn default() -> Self {
        Self {
            nlist: 0,
            nprobe: DEFAULT_NPROBE,
            sample_bytes: DEFAULT_SAMPLE_BYTES,
        }
    }
}

impl IvfFlatParams {
    pub fn validate(&self) -> Result<()> {
        for (name, value, max) in [
            ("nlist", self.nlist, MAX_NLIST),
            ("nprobe", self.nprobe, MAX_NLIST),
        ] {
            if value > max {
                return Err(Error::Other(format!(
                    "vector index {name} is {value}, above the maximum {max}"
                )));
            }
        }
        if self.sample_bytes == 0 {
            return Err(Error::Other(
                "vector index sampleBytes must leave room for a training sample".into(),
            ));
        }
        Ok(())
    }

    /// `4*sqrt(n)` is the usual starting point: lists stay large enough to
    /// train and small enough that probing a few is much cheaper than a scan.
    pub fn resolved_nlist(&self, corpus: u64) -> u32 {
        ivf::resolved_nlist(self.nlist, corpus)
    }

    /// Vectors needed before training fires. See
    /// [`ivf::resolved_train_threshold`] for why a derived `nlist` still
    /// yields a threshold independent of the corpus.
    pub fn resolved_train_threshold(&self) -> u64 {
        ivf::resolved_train_threshold(self.nlist)
    }
}
