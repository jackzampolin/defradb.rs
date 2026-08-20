//! Binary values reach storage as bytes, not as text.
//!
//! GraphQL variables are JSON, which has no binary type, so writing bytes
//! through a query means hex or base64 — double or a third again on every
//! write, in storage, and on the wire. `doc_mutator` and `doc_fetcher` skip
//! that boundary. These tests pin both directions and the fact that the JSON
//! rendering is a view, not the stored form.

use defra_node::{Document, EmbeddedNode, NormalValue};

const SDL: &str = "type Payload { name: String @index(unique: true) data: Blob }";

async fn node() -> EmbeddedNode {
    let node = EmbeddedNode::builder()
        .build()
        .await
        .expect("build in-memory node");
    node.add_schema(SDL).await.expect("add schema");
    node
}

/// Every byte value, including those no text encoding survives by accident.
fn payload() -> Vec<u8> {
    (0..=255u8).cycle().take(4096).collect()
}

#[tokio::test]
async fn bytes_written_typed_come_back_typed() {
    let node = node().await;
    let collection = node
        .get_collection("Payload")
        .expect("look up collection")
        .expect("collection exists");

    let mut doc = Document::with_collection(collection);
    doc.set("name", "round-trip");
    doc.set("data", NormalValue::Bytes(payload()));

    node.doc_mutator()
        .create("Payload", doc)
        .await
        .expect("create through the typed seam");

    let fetched = node
        .doc_fetcher()
        .get_by_field_value("Payload", "name", "round-trip")
        .await
        .expect("fetch by field value");
    assert_eq!(fetched.len(), 1, "expected exactly one document");

    match fetched[0].get("data") {
        Some(NormalValue::Bytes(bytes)) => assert_eq!(bytes, &payload()),
        other => panic!("expected NormalValue::Bytes, got {other:?}"),
    }
}

/// The hex a query returns is a JSON rendering of stored bytes. If this ever
/// came back as raw text the value was never binary in the first place.
#[tokio::test]
async fn the_same_value_renders_as_hex_through_graphql() {
    let node = node().await;
    let collection = node
        .get_collection("Payload")
        .expect("look up collection")
        .expect("collection exists");

    let mut doc = Document::with_collection(collection);
    doc.set("name", "rendered");
    doc.set("data", NormalValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    node.doc_mutator()
        .create("Payload", doc)
        .await
        .expect("create through the typed seam");

    let response = node
        .execute(r#"query { Payload(filter: {name: {_eq: "rendered"}}) { data } }"#)
        .await;
    assert!(response.errors.is_empty(), "errors: {:?}", response.errors);

    let rendered = response
        .data
        .as_ref()
        .and_then(|data| data.get("Payload"))
        .and_then(|rows| rows.get(0))
        .and_then(|row| row.get("data"))
        .and_then(|value| value.as_str())
        .expect("data field present");
    assert_eq!(rendered, "deadbeef");
}
