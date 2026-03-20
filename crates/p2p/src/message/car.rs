//! CAR fetch request types used by transport-specific CAR transfer paths.

use std::collections::HashSet;

use cid::Cid;
use serde::{Deserialize, Serialize};

/// Request for fetching blocks packaged as a CAR response.
///
/// `root_cid` is the DAG root being synchronized for correlation and logging.
/// `wanted_cids` are the CAR roots to include in the response.
/// If `recursive` is true, the responder walks the DAG from `root_cid`.
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

    pub fn response_roots(&self) -> Vec<Cid> {
        if self.recursive || self.wanted_cids.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::multihash::{Code, MultihashDigest};

    fn make_cid(label: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(label);
        Cid::new_v1(0x71, hash)
    }

    #[test]
    fn full_dag_request_sets_recursive_root() {
        let root = make_cid(b"root");
        let req = CarFetchRequest::full_dag(root);
        assert!(req.recursive);
        assert_eq!(req.root_cid, root);
        assert_eq!(req.wanted_cids, vec![root]);
        assert_eq!(req.response_roots(), vec![root]);
    }

    #[test]
    fn selective_request_dedupes_roots() {
        let root = make_cid(b"root");
        let child_a = make_cid(b"child-a");
        let child_b = make_cid(b"child-b");
        let req = CarFetchRequest::selective_blocks(root, vec![child_a, child_b, child_a]);
        assert!(!req.recursive);
        assert_eq!(req.root_cid, root);
        assert_eq!(req.wanted_cids, vec![child_a, child_b]);
        assert_eq!(req.response_roots(), vec![child_a, child_b]);
    }
}
