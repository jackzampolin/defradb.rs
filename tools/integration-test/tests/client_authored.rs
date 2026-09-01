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

use cid::Cid;
use crypto::keys::PrivateKey;
use defra_core::block::{
    Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload, Signature, SignatureHeader,
    SignatureType,
};
use document::{DocID, NormalValue};
use integration_test::TestCluster;
use serde_json::{json, Value};

const SCHEMA: &str = "type Reading { device: String  seq: Int  centicelsius: Int }";

/// Read is owner-or-granted, which is what makes ownership observable: an
/// unregistered document is public, a registered one is not.
const POLICY: &str = r#"name: client-authored-policy
description: Ownership of documents pushed as signed commit fragments

resources:
  - name: users
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor"#;

// ---------------------------------------------------------------------------
// Building a signed fragment, the way an external client would
// ---------------------------------------------------------------------------

/// A create, ready to push: the blocks, and the document ID they imply.
struct Fragment {
    doc_id: String,
    genesis: Cid,
    blocks: Vec<(Cid, Vec<u8>)>,
}

impl Fragment {
    fn wire(&self, collection_id: &str) -> Value {
        json!({
            "doc_id": self.doc_id,
            "collection_id": collection_id,
            "roots": [self.genesis.to_string()],
            "blocks": self.blocks.iter().map(|(cid, bytes)| json!({
                "cid": cid.to_string(),
                "data": hex::encode(bytes),
            })).collect::<Vec<_>>(),
        })
    }
}

fn encode(block: &Block) -> (Cid, Vec<u8>) {
    let bytes = block.to_dag_cbor().expect("block encodes");
    (
        defra_core::block::generate_cid_from_bytes(&bytes).expect("block CID"),
        bytes,
    )
}

/// Build the field and composite blocks for a create, and sign the composite.
///
/// Only the composite is signed. That is sufficient by construction: its
/// links are content addresses, so a signature over it already commits to the
/// exact bytes of every field block it names. `sign_fields` exists to prove
/// the node is equally happy either way.
fn build(
    fields: &[(&str, NormalValue)],
    version_id: &str,
    signer: Option<&crypto::Secp256k1PrivateKey>,
    sign_fields: bool,
) -> Fragment {
    let mut blocks = Vec::new();
    let mut links = Vec::new();

    for (name, value) in fields {
        let mut data = Vec::new();
        ciborium::into_writer(value, &mut data).expect("value encodes");
        let mut field = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: (*name).to_string(),
                priority: 1,
                schema_version_id: version_id.to_string(),
                data,
            }),
            vec![],
            vec![],
        );

        if sign_fields {
            let key = signer.expect("signing fields needs a key");
            let (sig_cid, sig_bytes) = sign(&field, key);
            blocks.push((sig_cid, sig_bytes));
            field.signature = Some(sig_cid);
        }

        let (cid, bytes) = encode(&field);
        links.push(DAGLink::new(*name, cid));
        blocks.push((cid, bytes));
    }

    let composite = |signature: Option<Cid>| {
        let mut block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: version_id.to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            links.clone(),
        );
        block.signature = signature;
        block
    };

    let signature_cid = signer.map(|key| {
        // The preimage is the composite without its own signature link, which
        // is what the node reconstructs when it verifies.
        let (sig_cid, sig_bytes) = sign(&composite(None), key);
        blocks.push((sig_cid, sig_bytes.clone()));
        sig_cid
    });

    let (genesis, genesis_bytes) = encode(&composite(signature_cid));
    blocks.push((genesis, genesis_bytes));

    Fragment {
        doc_id: DocID::new_v0(genesis).to_string(),
        genesis,
        blocks,
    }
}

fn sign(block: &Block, key: &crypto::Secp256k1PrivateKey) -> (Cid, Vec<u8>) {
    let mut unsigned = block.clone();
    unsigned.signature = None;
    let preimage = unsigned.to_dag_cbor().expect("preimage encodes");

    let signature = Signature::new(
        SignatureHeader::new(
            SignatureType::ES256K,
            hex::encode(key.public_key().raw()).into_bytes(),
        ),
        key.sign(&preimage).expect("signs"),
    );
    let bytes = signature.to_dag_cbor().expect("signature encodes");
    (
        defra_core::block::generate_cid_from_bytes(&bytes).expect("signature CID"),
        bytes,
    )
}

/// A keypair and the bearer token that speaks for it.
///
/// Generated in process rather than through the CLI: these tests are about the
/// HTTP contract, and shelling out would make them depend on the operator's
/// `~/.defradb` config.
struct Author {
    key: crypto::Secp256k1PrivateKey,
    node_identity: identity::RawIdentity,
    public_key_hex: String,
}

fn actor(seed: u8) -> Author {
    let secret = [seed; 32];
    let key = crypto::Secp256k1PrivateKey::from_bytes(&secret).expect("a valid scalar");
    let node_identity = identity::RawIdentity::from_identity_key_type(
        identity::IdentityKeyType::Secp256k1,
        &secret,
    )
    .expect("the node derives the same identity");

    Author {
        public_key_hex: hex::encode(key.public_key().raw()),
        key,
        node_identity,
    }
}

impl Author {
    /// A bearer token for `audience`, which must equal the Host the request
    /// will carry or the node refuses it.
    fn bearer(&self, audience: &str) -> String {
        let token = identity::new_token(
            &self.node_identity,
            std::time::Duration::from_secs(3600),
            Some(audience.to_string()),
            None,
        )
        .expect("mints a token");
        String::from_utf8(token).expect("a token is text")
    }
}

fn reading(seq: i64, centicelsius: i64) -> Vec<(&'static str, NormalValue)> {
    vec![
        ("device", NormalValue::String("sensor-7".into())),
        ("seq", NormalValue::Int(seq)),
        ("centicelsius", NormalValue::Int(centicelsius)),
    ]
}

// ---------------------------------------------------------------------------
// Talking to the node
// ---------------------------------------------------------------------------

struct Node {
    api: String,
    authority: String,
    http: reqwest::Client,
    collection_id: String,
    version_id: String,
}

impl Node {
    /// Start a node with the `Reading` collection. Passing an `owner` puts an
    /// access policy on it, which is what makes ownership observable.
    async fn start(owner: Option<&Author>) -> (TestCluster, Self) {
        let mut builder = TestCluster::builder().rust_nodes(1);
        if owner.is_some() {
            builder = builder.with_acp_local();
        }
        let cluster = builder.build().await.unwrap();
        let http = reqwest::Client::new();
        let api = cluster.api_url(0).to_string();
        let authority = api
            .trim_end_matches('/')
            .rsplit("://")
            .next()
            .expect("the API URL has an authority")
            .to_string();

        let schema = match owner {
            Some(owner) => {
                let policy: Value = http
                    .post(format!("{api}/api/v0/acp/policy"))
                    .bearer_auth(owner.bearer(&authority))
                    .body(POLICY)
                    .send()
                    .await
                    .expect("policy request")
                    .json()
                    .await
                    .expect("policy json");
                let policy_id = policy["PolicyID"]
                    .as_str()
                    .or_else(|| policy["policy_id"].as_str())
                    .unwrap_or_else(|| panic!("no PolicyID in {policy}"));
                format!(
                    "type Reading @policy(id: \"{policy_id}\", resource: \"users\") \
                     {{ device: String  seq: Int  centicelsius: Int }}"
                )
            }
            None => SCHEMA.to_string(),
        };

        let mut request = http
            .post(format!("{api}/api/v0/collections"))
            .body(schema.clone());
        if let Some(owner) = owner {
            request = request.bearer_auth(owner.bearer(&authority));
        }
        let response = request.send().await.expect("schema request");
        assert!(
            response.status().is_success(),
            "schema add failed: {}",
            response.text().await.unwrap_or_default()
        );

        let described: Value = http
            .get(format!("{api}/api/v0/collections/Reading/describe"))
            .send()
            .await
            .expect("describe request")
            .json()
            .await
            .expect("describe json");
        let described = described
            .as_array()
            .and_then(|versions| versions.first())
            .cloned()
            .unwrap_or(described);

        let node = Self {
            collection_id: described["CollectionID"].as_str().unwrap().to_string(),
            version_id: described["VersionID"].as_str().unwrap().to_string(),
            api,
            authority,
            http,
        };
        (cluster, node)
    }

    /// Push documents, returning the HTTP status and body so a test can assert
    /// on a refusal as easily as on a success.
    async fn sync(&self, documents: Vec<Value>, as_who: Option<&Author>) -> (u16, String) {
        let mut request = self
            .http
            .post(format!("{}/api/v0/sync", self.api))
            .json(&json!({ "documents": documents }));
        if let Some(who) = as_who {
            request = request.bearer_auth(who.bearer(&self.authority));
        }
        let response = request.send().await.expect("sync request");
        let status = response.status().as_u16();
        (status, response.text().await.unwrap_or_default())
    }

    async fn push_ok(&self, fragment: &Fragment, as_who: Option<&Author>) {
        let (status, body) = self
            .sync(vec![fragment.wire(&self.collection_id)], as_who)
            .await;
        assert!(
            (200..300).contains(&status),
            "the node refused a valid fragment: {status} {body}"
        );
    }

    async fn graphql(&self, query: &str, as_who: Option<&Author>) -> Value {
        let mut request = self
            .http
            .post(format!("{}/api/v0/graphql", self.api))
            .json(&json!({ "query": query }));
        if let Some(who) = as_who {
            request = request.bearer_auth(who.bearer(&self.authority));
        }
        let body: Value = request
            .send()
            .await
            .expect("graphql request")
            .json()
            .await
            .expect("graphql json");
        assert!(
            body.get("errors")
                .is_none_or(|e| e.as_array().is_none_or(Vec::is_empty)),
            "graphql reported errors: {body}"
        );
        body["data"].clone()
    }

    /// `GET /block/signed` -- the block's stored bytes and its signature's,
    /// with nothing interpreted for the caller.
    async fn signed_block(&self, cid: &Cid) -> (u16, Value) {
        let response = self
            .http
            .get(format!("{}/api/v0/block/signed?cid={cid}", self.api))
            .send()
            .await
            .expect("signed block request");
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    /// Every signer the node recorded for a document, as `_commits` reports.
    async fn commit_signers(&self, doc_id: &str) -> Vec<String> {
        let data = self
            .graphql(
                &format!(
                    "query {{ _commits(docID: \"{doc_id}\") {{ signature {{ identity }} }} }}"
                ),
                None,
            )
            .await;
        data["_commits"]
            .as_array()
            .expect("a list of commits")
            .iter()
            .filter_map(|commit| commit["signature"]["identity"].as_str())
            .map(String::from)
            .collect()
    }
}

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
    let mut fragment = build(
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

    let genesis_block = fragment
        .blocks
        .iter()
        .find(|(cid, _)| *cid == fragment.genesis)
        .map(|(_, bytes)| Block::from_dag_cbor(bytes).unwrap())
        .unwrap();
    let signature_cid = genesis_block.signature.unwrap();
    let decoy_signature = decoy
        .blocks
        .iter()
        .find(|(_, bytes)| Signature::from_dag_cbor(bytes).is_ok())
        .map(|(_, bytes)| bytes.clone())
        .unwrap();

    for (cid, bytes) in fragment.blocks.iter_mut() {
        if *cid == signature_cid {
            *bytes = decoy_signature.clone();
        }
    }

    let (status, body) = node
        .sync(vec![fragment.wire(&node.collection_id)], None)
        .await;
    assert!(
        !(200..300).contains(&status),
        "the node accepted a signature that does not cover the block: {status} {body}"
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
