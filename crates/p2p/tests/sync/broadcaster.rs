use std::str::FromStr;

use cid::Cid;
use p2p::{Broadcaster, Libp2pTransport};

#[test]
fn test_create_broadcast() {
    let cid = Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let block = b"test block data";
    let doc_id = "bae-123";
    let collection_id = "users";
    let creator = "12D3KooWPeer";

    let broadcast = Broadcaster::<Libp2pTransport>::create_broadcast(
        &cid,
        block,
        doc_id,
        collection_id,
        creator,
    );

    assert_eq!(broadcast.doc_id, doc_id);
    assert_eq!(broadcast.collection_id, collection_id);
    assert_eq!(broadcast.creator, creator);
    assert_eq!(broadcast.block, block.to_vec());
    assert_eq!(broadcast.cid, cid.to_bytes());
}
