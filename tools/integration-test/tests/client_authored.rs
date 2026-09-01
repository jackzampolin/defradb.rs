//! Documents authored and signed by a client, pushed to `POST /api/v0/sync`.
//!
//! Over the ordinary write path the node builds the blocks, so the node signs
//! them -- with its own key, or with one it has been given. The commit's
//! signature then attests the node, not the writer. `/sync` is the path that
//! changes that: a client builds its own commit fragment, signs it with a key
//! the node never holds, and the node validates and merges rather than
//! authors.
//!
//! That makes the node's side of the bargain worth pinning down, because
//! everything a constrained client can promise rests on it:
//!
//! - the client's signature is what gets stored, not one the node substituted;
//! - ownership follows the *verified genesis signer*, never the caller who
//!   sent the request, so a relay can forward blocks it cannot claim;
//! - anything that does not verify is refused rather than quietly kept;
//! - `GET /api/v0/block/signed` hands back material a client can check for
//!   itself, without taking the node's word for any of it.
//!
//! The fragments here are built with the node's own crates rather than a
//! client library, so these tests describe the endpoint's contract and not
//! one particular consumer of it.

#[path = "client_authored_common.rs"]
mod client_authored_common;

use cid::Cid;
use defra_core::block::{Block, Signature};
use document::DocID;
use serde_json::json;

use client_authored_common::{
    actor, build, encode, genesis_block_of, reading, signature_block_of, Fragment, Node,
};

// ---------------------------------------------------------------------------
// What the node must do
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_client_signed_fragment_is_accepted_and_queryable() {
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;

    let fragment = build(&reading(1, 2137), &node.version_id, Some(key), false);

    // Three fields is five blocks: one per field, one signature, one
    // composite. No history, because a create has none.
    assert_eq!(fragment.blocks.len(), 5);
    node.push_ok(&fragment, None).await;

    let rows = node
        .graphql("query { Reading { _docID device seq centicelsius } }", None)
        .await["Reading"]
        .clone();
    let rows = rows.as_array().expect("rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_docID"], json!(fragment.doc_id));
    assert_eq!(rows[0]["device"], json!("sensor-7"));
    assert_eq!(rows[0]["seq"], json!(1));
    assert_eq!(rows[0]["centicelsius"], json!(2137));
}

#[tokio::test]
async fn the_stored_signature_is_the_clients_not_the_nodes() {
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;
    let author_key_hex = author.public_key_hex.clone();

    let fragment = build(&reading(1, 2137), &node.version_id, Some(key), false);
    node.push_ok(&fragment, None).await;

    assert!(
        node.commit_signers(&fragment.doc_id)
            .await
            .contains(&author_key_hex),
        "the node must keep the key the client signed with rather than substituting its own"
    );

    // The contrast: a document the node authored over the ordinary mutation
    // path carries whatever its own signing configuration produced, which is
    // the gap `/sync` exists to close.
    let created = node
        .graphql(
            r#"mutation { add_Reading(input: {device: "sensor-9", seq: 2, centicelsius: 1900}) { _docID } }"#,
            None,
        )
        .await;
    let node_doc_id = created["add_Reading"][0]["_docID"]
        .as_str()
        .expect("the mutation returns a docID");

    assert!(
        !node
            .commit_signers(node_doc_id)
            .await
            .contains(&author_key_hex),
        "a node-authored commit must not claim the client's key"
    );
}

#[tokio::test]
async fn a_composite_signature_alone_covers_every_field() {
    // DefraDB's own writer signs each field block as well. Nothing requires
    // it: the composite's links are content addresses, so one signature over
    // the composite commits to every field block it names. A constrained
    // client halves its blocks and its curve operations by relying on that,
    // so the node accepting both shapes is a contract worth pinning.
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;

    let composite_only = build(&reading(1, 2137), &node.version_id, Some(key), false);
    let every_block = build(&reading(2, 2138), &node.version_id, Some(key), true);

    assert_eq!(composite_only.blocks.len(), 5);
    assert_eq!(every_block.blocks.len(), 8, "three more signature blocks");

    node.push_ok(&composite_only, None).await;
    node.push_ok(&every_block, None).await;

    let rows = node.graphql("query { Reading { seq } }", None).await["Reading"].clone();
    assert_eq!(rows.as_array().expect("rows").len(), 2);
}

#[tokio::test]
async fn a_block_that_does_not_hash_to_its_cid_is_refused() {
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;

    let fragment = build(&reading(1, 100), &node.version_id, Some(key), false);
    let mut wire = fragment.wire(&node.collection_id);

    // Alter a field's value while leaving its advertised CID alone: the
    // tamper a dishonest relay would attempt on a client's readings.
    let genesis = fragment.genesis.to_string();
    let blocks = wire["blocks"].as_array_mut().unwrap();
    let target = blocks
        .iter_mut()
        .find(|block| {
            block["cid"] != json!(genesis) && block["data"].as_str().unwrap().contains("1864")
        })
        .expect("the centicelsius field block is on the wire");
    let tampered = target["data"].as_str().unwrap().replace("1864", "1865");
    target["data"] = json!(tampered);

    let (status, body) = node.sync(vec![wire], None).await;
    assert!(
        !(200..300).contains(&status),
        "the node accepted a block that does not hash to its CID: {status} {body}"
    );
}

#[tokio::test]
async fn a_forged_signature_is_refused() {
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let impostor = actor(0x22);

    // A genuine fragment, then its signature block swapped for one the
    // impostor made over different content. The header still names the
    // impostor, so this is not a key confusion: the signature simply does not
    // cover these bytes.
    let fragment = build(
        &reading(1, 2137),
        &node.version_id,
        Some(&author.key),
        false,
    );
    let decoy = build(
        &reading(99, 1),
        &node.version_id,
        Some(&impostor.key),
        false,
    );

    // Swap in the impostor's signature block *and* relink the genesis to it,
    // so every block still hashes to the CID it is filed under. Replacing the
    // bytes alone would be caught by CID validation first, and the test would
    // pass without signature verification ever running.
    let (decoy_signature_cid, decoy_signature) = signature_block_of(&decoy);
    let genesis_block = genesis_block_of(&fragment);
    let author_signature_cid = genesis_block.signature.expect("the author signed");

    let mut forged = genesis_block;
    forged.signature = Some(decoy_signature_cid);
    let (forged_cid, forged_bytes) = encode(&forged);

    // Keep the field blocks; drop the author's genesis and signature.
    let genesis_cid = fragment.genesis;
    let mut blocks: Vec<(Cid, Vec<u8>)> = fragment
        .blocks
        .into_iter()
        .filter(|(cid, _)| *cid != genesis_cid && *cid != author_signature_cid)
        .collect();
    blocks.push((decoy_signature_cid, decoy_signature));
    blocks.push((forged_cid, forged_bytes));

    let fragment = Fragment {
        doc_id: DocID::new_v0(forged_cid).to_string(),
        genesis: forged_cid,
        blocks,
    };

    let (status, body) = node
        .sync(vec![fragment.wire(&node.collection_id)], None)
        .await;
    assert!(
        !(200..300).contains(&status),
        "the node accepted a signature that does not cover the block: {status} {body}"
    );
    assert!(
        !body.contains("does not match claimed CID"),
        "this must fail signature verification, not CID validation: {body}"
    );
}

#[tokio::test]
async fn a_document_id_that_does_not_match_its_genesis_is_refused() {
    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;

    let fragment = build(&reading(1, 2137), &node.version_id, Some(key), false);
    let other = build(&reading(2, 2138), &node.version_id, Some(key), false);

    // Genuine blocks, genuinely signed -- filed under someone else's ID.
    let mut wire = fragment.wire(&node.collection_id);
    wire["doc_id"] = json!(other.doc_id);

    let (status, body) = node.sync(vec![wire], None).await;
    assert!(
        !(200..300).contains(&status),
        "a document ID must be checked against its genesis block: {status} {body}"
    );
}

#[tokio::test]
async fn block_signed_returns_material_a_client_can_check_for_itself() {
    use base64::Engine as _;

    let (_cluster, node) = Node::start(None).await;
    let author = actor(0x11);
    let key = &author.key;

    let fragment = build(&reading(1, 2137), &node.version_id, Some(key), false);
    node.push_ok(&fragment, None).await;

    let (status, body) = node.signed_block(&fragment.genesis).await;
    assert_eq!(status, 200, "the block should be servable: {body}");

    let engine = base64::engine::general_purpose::STANDARD;
    let block_bytes = engine
        .decode(body["block"].as_str().expect("block"))
        .unwrap();
    let signature_bytes = engine
        .decode(body["signature"].as_str().expect("signature"))
        .unwrap();

    // Everything a client must be able to do without trusting the answer.
    let rederived = defra_core::block::generate_cid_from_bytes(&block_bytes).unwrap();
    assert_eq!(
        rederived, fragment.genesis,
        "the bytes must name the CID asked for"
    );

    let block = Block::from_dag_cbor(&block_bytes).unwrap();
    assert_eq!(
        DocID::new_v0(rederived).to_string(),
        fragment.doc_id,
        "the block served must be the genesis of the document that was pushed"
    );

    let mut unsigned = block.clone();
    unsigned.signature = None;
    let preimage = unsigned.to_dag_cbor().unwrap();

    let signature = Signature::from_dag_cbor(&signature_bytes).unwrap();
    let identity = String::from_utf8(signature.header.identity.clone()).unwrap();
    assert_eq!(identity, author.public_key_hex);

    let public = crypto::public_key_from_string(crypto::KeyType::Secp256k1, &identity).unwrap();
    assert!(
        public.verify(&preimage, &signature.value).unwrap(),
        "the served signature must verify over the served bytes"
    );

    // And a CID the node does not hold is an error, not a silent empty.
    let absent = defra_core::block::generate_cid_from_bytes(b"never stored").unwrap();
    let (status, _) = node.signed_block(&absent).await;
    assert_ne!(status, 200, "an unknown CID must not answer 200");
}

// ---------------------------------------------------------------------------
// Ownership: the property a relay must not be able to subvert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ownership_binds_to_the_signature_not_the_pusher() {
    // The headline guarantee. A gateway forwards blocks its devices signed; it
    // authenticates as itself, and must not thereby become their author. If
    // ownership followed the caller, every fleet would be one compromised
    // relay away from forged provenance.
    let author = actor(0x11);
    let relay = actor(0x22);
    let (_cluster, node) = Node::start(Some(&author)).await;

    let fragment = build(
        &reading(1, 2137),
        &node.version_id,
        Some(&author.key),
        false,
    );

    // The relay pushes, authenticated as the relay. Nothing it sends says who
    // signed; the node has to work that out from the genesis block itself.
    node.push_ok(&fragment, Some(&relay)).await;

    let seen_by_author = node
        .graphql("query { Reading { _docID } }", Some(&author))
        .await["Reading"]
        .as_array()
        .expect("rows")
        .len();
    let seen_by_relay = node
        .graphql("query { Reading { _docID } }", Some(&relay))
        .await["Reading"]
        .as_array()
        .expect("rows")
        .len();

    assert_eq!(
        seen_by_author, 1,
        "the signer must own the document it signed"
    );
    assert_eq!(
        seen_by_relay, 0,
        "the pusher must not become the owner of blocks it merely carried"
    );
}

#[tokio::test]
async fn an_unsigned_genesis_leaves_the_document_unregistered() {
    // An unsigned genesis has no verifiable author, so there is nobody to
    // register as owner and the document stays public under local ACP. That
    // is the documented semantics, and it is precisely why signing is not
    // optional for a client that cares who its data belongs to.
    let author = actor(0x11);
    let outsider = actor(0x33);
    let (_cluster, node) = Node::start(Some(&author)).await;

    let unsigned = build(&reading(1, 2137), &node.version_id, None, false);
    assert_eq!(unsigned.blocks.len(), 4, "no signature block");
    node.push_ok(&unsigned, Some(&author)).await;

    let seen_by_outsider = node
        .graphql("query { Reading { _docID } }", Some(&outsider))
        .await["Reading"]
        .as_array()
        .expect("rows")
        .len();

    assert_eq!(
        seen_by_outsider, 1,
        "an unregistered document is public: unsigned means unowned, not private"
    );
}
