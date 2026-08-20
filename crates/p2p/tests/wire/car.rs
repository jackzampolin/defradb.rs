use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use p2p::message::CarFetchRequest;

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

#[test]
fn selective_dag_walks_only_from_deduped_missing_frontier() {
    let root = make_cid(b"root");
    let child_a = make_cid(b"child-a");
    let child_b = make_cid(b"child-b");
    let req = CarFetchRequest::selective_dag(root, vec![child_a, child_b, child_a]);

    assert!(req.recursive);
    assert_eq!(req.root_cid, root);
    assert_eq!(req.wanted_cids, vec![child_a, child_b]);
    assert_eq!(req.response_roots(), vec![child_a, child_b]);
}
