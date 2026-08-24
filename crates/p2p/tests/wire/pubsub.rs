use p2p::message::pubsub::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncItem, DocSyncReply, DocSyncRequest,
    MAX_BRANCH_HEADS, MAX_DOC_IDS, MAX_HEADS_PER_DOC,
};

// Byte fixtures produced by `testdata/gen_message_fixtures/main.go`, which
// runs `cbor.Marshal(...)` from `github.com/fxamacker/cbor/v2` with default
// opts — the same pipeline as `defradb/internal/db/p2p/sync_doc.go:112,
// :303` and `sync_branchable_col.go:107, :271`.
//
// To regenerate:
//   cd testdata/gen_message_fixtures && go run main.go
const GO_DOC_SYNC_REQUEST_TWO_IDS_HEX: &str = "a166646f634944738264646f634164646f6342";
const GO_DOC_SYNC_REQUEST_EMPTY_HEX: &str = "a166646f6349447380";
const GO_DOC_SYNC_ITEM_HEX: &str =
    "a265646f6349446b626166792d646f632d6964656865616473824301020344ffeeddcc";
const GO_DOC_SYNC_REPLY_HEX: &str = "a267726573756c747382a265646f63494466626166792d316568656164738144deadbeefa265646f63494466626166792d32656865616473814200116673656e6465726c313244334b6f6f5750656572";
const GO_DOC_SYNC_REPLY_EMPTY_HEX: &str = "a267726573756c7473f66673656e6465726470656572";
const GO_BRANCHABLE_SYNC_REQUEST_HEX: &str =
    "a16c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e";
const GO_BRANCHABLE_SYNC_REPLY_HEX: &str = "a36c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e6568656164738243aabbcc4299886673656e6465726c313244334b6f6f5750656572";
const GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX: &str =
    "a36c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e656865616473f66673656e6465726470656572";

// ---------- encode parity ----------

#[test]
fn doc_sync_request_two_ids_matches_go_fixture() {
    let req = DocSyncRequest::new(vec!["docA".into(), "docB".into()]);
    assert_hex_eq(encode(&req), GO_DOC_SYNC_REQUEST_TWO_IDS_HEX);
}

#[test]
fn doc_sync_request_empty_matches_go_fixture() {
    let req = DocSyncRequest::new(vec![]);
    assert_hex_eq(encode(&req), GO_DOC_SYNC_REQUEST_EMPTY_HEX);
}

#[test]
fn doc_sync_item_matches_go_fixture() {
    let item = DocSyncItem {
        doc_id: "bafy-doc-id".into(),
        heads: vec![vec![0x01, 0x02, 0x03], vec![0xff, 0xee, 0xdd, 0xcc]],
    };
    assert_hex_eq(encode(&item), GO_DOC_SYNC_ITEM_HEX);
}

#[test]
fn doc_sync_reply_matches_go_fixture() {
    let reply = DocSyncReply {
        results: vec![
            DocSyncItem {
                doc_id: "bafy-1".into(),
                heads: vec![vec![0xde, 0xad, 0xbe, 0xef]],
            },
            DocSyncItem {
                doc_id: "bafy-2".into(),
                heads: vec![vec![0x00, 0x11]],
            },
        ],
        sender: "12D3KooWPeer".into(),
    };
    assert_hex_eq(encode(&reply), GO_DOC_SYNC_REPLY_HEX);
}

#[test]
fn doc_sync_reply_empty_results_emits_null_like_go() {
    let reply = DocSyncReply {
        results: vec![],
        sender: "peer".into(),
    };
    assert_hex_eq(encode(&reply), GO_DOC_SYNC_REPLY_EMPTY_HEX);
}

#[test]
fn branchable_sync_request_matches_go_fixture() {
    let req = BranchableSyncRequest::new("bafy-collection".into());
    assert_hex_eq(encode(&req), GO_BRANCHABLE_SYNC_REQUEST_HEX);
}

#[test]
fn branchable_sync_reply_matches_go_fixture() {
    let reply = BranchableSyncReply {
        collection_id: "bafy-collection".into(),
        heads: vec![vec![0xaa, 0xbb, 0xcc], vec![0x99, 0x88]],
        sender: "12D3KooWPeer".into(),
    };
    assert_hex_eq(encode(&reply), GO_BRANCHABLE_SYNC_REPLY_HEX);
}

#[test]
fn branchable_sync_reply_empty_heads_emits_null_like_go() {
    let reply = BranchableSyncReply {
        collection_id: "bafy-collection".into(),
        heads: vec![],
        sender: "peer".into(),
    };
    assert_hex_eq(encode(&reply), GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX);
}

// ---------- decode parity ----------

#[test]
fn decodes_go_doc_sync_request_two_ids() {
    let bytes = hex::decode(GO_DOC_SYNC_REQUEST_TWO_IDS_HEX).unwrap();
    let decoded: DocSyncRequest = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert_eq!(decoded.doc_ids, vec!["docA", "docB"]);
}

#[test]
fn decodes_go_doc_sync_reply_with_items() {
    let bytes = hex::decode(GO_DOC_SYNC_REPLY_HEX).unwrap();
    let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert_eq!(decoded.results.len(), 2);
    assert_eq!(decoded.results[0].doc_id, "bafy-1");
    assert_eq!(decoded.results[0].heads, vec![vec![0xde, 0xad, 0xbe, 0xef]]);
    assert_eq!(decoded.sender, "12D3KooWPeer");
}

#[test]
fn decodes_go_doc_sync_reply_null_as_empty_vec() {
    let bytes = hex::decode(GO_DOC_SYNC_REPLY_EMPTY_HEX).unwrap();
    let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert!(decoded.results.is_empty());
    assert_eq!(decoded.sender, "peer");
}

#[test]
fn decodes_go_branchable_sync_reply_null_heads_as_empty() {
    let bytes = hex::decode(GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX).unwrap();
    let decoded: BranchableSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert_eq!(decoded.collection_id, "bafy-collection");
    assert!(decoded.heads.is_empty());
    assert_eq!(decoded.sender, "peer");
}

// ---------- bounded-vec enforcement ----------

#[test]
fn doc_sync_request_rejects_oversized_doc_ids() {
    #[derive(serde::Serialize)]
    struct Raw {
        #[serde(rename = "docIDs")]
        doc_ids: Vec<String>,
    }
    let raw = Raw {
        doc_ids: (0..=MAX_DOC_IDS).map(|i| format!("doc-{i}")).collect(),
    };
    let bytes = encode(&raw);
    let err = ciborium::from_reader::<DocSyncRequest, _>(bytes.as_slice())
        .expect_err("must reject oversized payload");
    assert!(err.to_string().contains("docIDs"), "{err}");
}

#[test]
fn doc_sync_request_accepts_boundary_doc_ids() {
    #[derive(serde::Serialize)]
    struct Raw {
        #[serde(rename = "docIDs")]
        doc_ids: Vec<String>,
    }
    let raw = Raw {
        doc_ids: (0..MAX_DOC_IDS).map(|i| format!("doc-{i}")).collect(),
    };
    let bytes = encode(&raw);
    let decoded: DocSyncRequest =
        ciborium::from_reader(bytes.as_slice()).expect("exactly MAX_DOC_IDS must decode");
    assert_eq!(decoded.doc_ids.len(), MAX_DOC_IDS);
}

#[test]
fn doc_sync_reply_rejects_oversized_results() {
    #[derive(serde::Serialize)]
    struct Raw {
        #[serde(rename = "results")]
        results: Vec<DocSyncItem>,
        #[serde(rename = "sender")]
        sender: String,
    }
    let raw = Raw {
        results: (0..=MAX_DOC_IDS)
            .map(|i| DocSyncItem {
                doc_id: format!("doc-{i}"),
                heads: vec![vec![0u8]],
            })
            .collect(),
        sender: "peer".into(),
    };
    let bytes = encode(&raw);
    let err = ciborium::from_reader::<DocSyncReply, _>(bytes.as_slice())
        .expect_err("must reject oversized results");
    assert!(err.to_string().contains("results"), "{err}");
}

#[test]
fn doc_sync_item_rejects_oversized_heads() {
    #[derive(serde::Serialize)]
    struct Raw<'a> {
        #[serde(rename = "docID")]
        doc_id: String,
        #[serde(rename = "heads")]
        heads: Vec<&'a serde_bytes::Bytes>,
    }
    let big: Vec<Vec<u8>> = (0..=MAX_HEADS_PER_DOC).map(|_| vec![0u8]).collect();
    let raw = Raw {
        doc_id: "d".into(),
        heads: big.iter().map(|v| serde_bytes::Bytes::new(v)).collect(),
    };
    let bytes = encode(&raw);
    let err = ciborium::from_reader::<DocSyncItem, _>(bytes.as_slice())
        .expect_err("must reject oversized heads");
    assert!(err.to_string().contains("heads-per-doc"), "{err}");
}

#[test]
fn branchable_sync_reply_rejects_oversized_heads() {
    #[derive(serde::Serialize)]
    struct Raw<'a> {
        #[serde(rename = "collectionID")]
        collection_id: String,
        #[serde(rename = "heads")]
        heads: Vec<&'a serde_bytes::Bytes>,
        #[serde(rename = "sender")]
        sender: String,
    }
    let big: Vec<Vec<u8>> = (0..=MAX_BRANCH_HEADS).map(|_| vec![0u8]).collect();
    let raw = Raw {
        collection_id: "c".into(),
        heads: big.iter().map(|v| serde_bytes::Bytes::new(v)).collect(),
        sender: "peer".into(),
    };
    let bytes = encode(&raw);
    let err = ciborium::from_reader::<BranchableSyncReply, _>(bytes.as_slice())
        .expect_err("must reject oversized heads");
    assert!(err.to_string().contains("branchable"), "{err}");
}

// ---------- round-trip ----------

#[test]
fn round_trip_doc_sync_reply_with_items() {
    let original = DocSyncReply {
        results: vec![DocSyncItem {
            doc_id: "bafy".into(),
            heads: vec![vec![0x00], vec![0x01, 0xff]],
        }],
        sender: "peerA".into(),
    };
    let bytes = encode(&original);
    let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).expect("decode");
    assert_eq!(decoded, original);
}

#[test]
fn round_trip_branchable_sync_reply_with_heads() {
    let original = BranchableSyncReply {
        collection_id: "col".into(),
        heads: vec![vec![0xaa], vec![0xbb, 0xcc]],
        sender: "peer".into(),
    };
    let bytes = encode(&original);
    let decoded: BranchableSyncReply = ciborium::from_reader(bytes.as_slice()).expect("decode");
    assert_eq!(decoded, original);
}

// ---------- helpers ----------

fn encode<T: serde::Serialize>(v: &T) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(v, &mut out).expect("encode");
    out
}

fn assert_hex_eq(got: Vec<u8>, expected_hex: &str) {
    let got_hex = hex::encode(&got);
    assert_eq!(
        got_hex,
        expected_hex,
        "byte mismatch vs Go fixture (Rust len={}, Go len={})",
        got.len(),
        expected_hex.len() / 2
    );
}
