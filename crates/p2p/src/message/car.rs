//! CAR fetch request types used by transport-specific CAR transfer paths.

use std::collections::HashSet;

use cid::Cid;
use serde::{Deserialize, Serialize};

/// Request for fetching blocks packaged as a CAR response.
///
/// `root_cid` is the DAG root being synchronized for correlation and logging.
/// `wanted_cids` are the CAR roots to include in the response.
/// If `recursive` is true, the responder walks the DAG from `wanted_cids`
/// (falling back to `root_cid` for legacy requests with no want list).
/// If `recursive` is false, the responder returns only the explicitly wanted blocks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CarFetchRequest {
    pub root_cid: Cid,
    #[serde(default)]
    pub wanted_cids: Vec<Cid>,
    #[serde(default = "default_recursive")]
    pub recursive: bool,
}

fn default_recursive() -> bool {
    true
}

impl CarFetchRequest {
    pub fn full_dag(root_cid: Cid) -> Self {
        Self {
            root_cid,
            wanted_cids: vec![root_cid],
            recursive: true,
        }
    }

    pub fn selective_blocks(root_cid: Cid, wanted_cids: Vec<Cid>) -> Self {
        Self {
            root_cid,
            wanted_cids: dedupe_cids(wanted_cids),
            recursive: false,
        }
    }

    /// Fetch a bounded descendant closure rooted at the known missing
    /// frontier. This is distinct from a historical walk from `root_cid`:
    /// every traversal root is a CID the receiver already proved missing.
    pub fn selective_dag(root_cid: Cid, wanted_cids: Vec<Cid>) -> Self {
        Self {
            root_cid,
            wanted_cids: dedupe_cids(wanted_cids),
            recursive: true,
        }
    }

    pub fn response_roots(&self) -> Vec<Cid> {
        if self.wanted_cids.is_empty() {
            vec![self.root_cid]
        } else {
            self.wanted_cids.clone()
        }
    }
}

fn dedupe_cids(cids: Vec<Cid>) -> Vec<Cid> {
    let mut seen = HashSet::with_capacity(cids.len());
    cids.into_iter().filter(|cid| seen.insert(*cid)).collect()
}
