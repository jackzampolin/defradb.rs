//! Building a signed commit fragment the way an external client would, and
//! talking to a node about it.
//!
//! Shared by `client_authored` (the endpoint's contract on one node) and
//! `p2p_iroh::replication::client_authored` (what happens to a pushed
//! fragment once the node it landed on has peers).
//!
//! The fragments are built with the node's own crates rather than a client
//! library, so these helpers describe the endpoint and not one particular
//! consumer of it.

#![allow(dead_code)]

use cid::Cid;
use crypto::keys::PrivateKey;
use defra_core::block::{
    Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload, Signature, SignatureHeader,
    SignatureType,
};
use document::{DocID, NormalValue};
use integration_test::TestCluster;
use serde_json::{json, Value};

pub const SCHEMA: &str = "type Reading { device: String  seq: Int  centicelsius: Int }";

/// Read is owner-or-granted, which is what makes ownership observable: an
/// unregistered document is public, a registered one is not.
pub const POLICY: &str = r#"name: client-authored-policy
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
pub struct Fragment {
    pub doc_id: String,
    pub genesis: Cid,
    pub blocks: Vec<(Cid, Vec<u8>)>,
}

impl Fragment {
    pub fn wire(&self, collection_id: &str) -> Value {
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

pub fn encode(block: &Block) -> (Cid, Vec<u8>) {
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
pub fn build(
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

pub fn sign(block: &Block, key: &crypto::Secp256k1PrivateKey) -> (Cid, Vec<u8>) {
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
pub struct Author {
    pub key: crypto::Secp256k1PrivateKey,
    pub node_identity: identity::RawIdentity,
    pub public_key_hex: String,
}

pub fn actor(seed: u8) -> Author {
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
    pub fn bearer(&self, audience: &str) -> String {
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

/// The genesis composite of a fragment, decoded.
pub fn genesis_block_of(fragment: &Fragment) -> Block {
    fragment
        .blocks
        .iter()
        .find(|(cid, _)| *cid == fragment.genesis)
        .map(|(_, bytes)| Block::from_dag_cbor(bytes).expect("genesis decodes"))
        .expect("the genesis block is in the fragment")
}

/// The signature block a fragment's genesis links to.
pub fn signature_block_of(fragment: &Fragment) -> (Cid, Vec<u8>) {
    let cid = genesis_block_of(fragment)
        .signature
        .expect("a signed fragment links its signature");
    fragment
        .blocks
        .iter()
        .find(|(candidate, _)| *candidate == cid)
        .cloned()
        .expect("the signature block is in the fragment")
}

pub fn reading(seq: i64, centicelsius: i64) -> Vec<(&'static str, NormalValue)> {
    vec![
        ("device", NormalValue::String("sensor-7".into())),
        ("seq", NormalValue::Int(seq)),
        ("centicelsius", NormalValue::Int(centicelsius)),
    ]
}

// ---------------------------------------------------------------------------
// Talking to the node
// ---------------------------------------------------------------------------

pub struct Node {
    pub api: String,
    pub authority: String,
    pub http: reqwest::Client,
    pub collection_id: String,
    pub version_id: String,
}

impl Node {
    /// Start a node with the `Reading` collection. Passing an `owner` puts an
    /// access policy on it, which is what makes ownership observable.
    pub async fn start(owner: Option<&Author>) -> (TestCluster, Self) {
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

        (cluster, Self::attach(api).await)
    }

    /// Talk to a node whose `Reading` collection is already deployed -- one
    /// the P2P helpers built, say. Only the collection identifiers are read,
    /// and both nodes of a pair derive the same ones from the same schema.
    pub async fn attach(api: String) -> Self {
        let http = reqwest::Client::new();
        let authority = api
            .trim_end_matches('/')
            .rsplit("://")
            .next()
            .expect("the API URL has an authority")
            .to_string();

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

        Self {
            collection_id: described["CollectionID"].as_str().unwrap().to_string(),
            version_id: described["VersionID"].as_str().unwrap().to_string(),
            api,
            authority,
            http,
        }
    }

    /// Push documents, returning the HTTP status and body so a test can assert
    /// on a refusal as easily as on a success.
    pub async fn sync(&self, documents: Vec<Value>, as_who: Option<&Author>) -> (u16, String) {
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

    pub async fn push_ok(&self, fragment: &Fragment, as_who: Option<&Author>) {
        let (status, body) = self
            .sync(vec![fragment.wire(&self.collection_id)], as_who)
            .await;
        assert!(
            (200..300).contains(&status),
            "the node refused a valid fragment: {status} {body}"
        );
    }

    pub async fn graphql(&self, query: &str, as_who: Option<&Author>) -> Value {
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
    pub async fn signed_block(&self, cid: &Cid) -> (u16, Value) {
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
    pub async fn commit_signers(&self, doc_id: &str) -> Vec<String> {
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
