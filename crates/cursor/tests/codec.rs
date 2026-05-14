use cursor::{Cursor, CursorError};
use std::collections::BTreeMap;

#[test]
fn encode_decode_doc_id_only() {
    let c = Cursor::from_doc_id("doc-1");
    let token = c.encode();
    let decoded = Cursor::decode(&token).unwrap();
    assert_eq!(decoded.doc_id, "doc-1");
    assert!(decoded.keys.is_empty());
}

#[test]
fn encode_decode_with_keys() {
    let mut keys = BTreeMap::new();
    keys.insert("age".into(), serde_json::json!(30));
    keys.insert("name".into(), serde_json::json!("alice"));
    let c = Cursor {
        doc_id: "doc-1".into(),
        keys: keys.clone(),
    };

    let token = c.encode();
    let decoded = Cursor::decode(&token).unwrap();
    assert_eq!(decoded.doc_id, "doc-1");
    assert_eq!(decoded.keys, keys);
}

#[test]
fn decode_rejects_invalid_base64() {
    let err = Cursor::decode("!!!not-base64!!!").unwrap_err();
    assert!(matches!(err, CursorError::InvalidBase64(_)));
}

#[test]
fn decode_rejects_invalid_json() {
    // base64url("not json") = "bm90IGpzb24"
    let token = "bm90IGpzb24";
    let err = Cursor::decode(token).unwrap_err();
    assert!(matches!(err, CursorError::InvalidJson(_)));
}

#[test]
fn decode_rejects_empty_doc_id() {
    // base64url('{"d":""}') = "eyJkIjoiIn0"
    let token = "eyJkIjoiIn0";
    let err = Cursor::decode(token).unwrap_err();
    assert!(matches!(err, CursorError::EmptyDocId));
}

#[test]
fn encode_omits_empty_keys() {
    let c = Cursor::from_doc_id("doc-1");
    let token = c.encode();
    // Decode the base64 and check JSON has no "k" field
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&token)
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        json.get("k").is_none(),
        "empty keys must be omitted from JSON"
    );
    assert_eq!(json.get("d").unwrap().as_str().unwrap(), "doc-1");
}

#[test]
fn keys_serialize_alphabetically() {
    let mut keys = BTreeMap::new();
    keys.insert("z_field".into(), serde_json::json!(1));
    keys.insert("a_field".into(), serde_json::json!(2));
    let c = Cursor {
        doc_id: "x".into(),
        keys,
    };

    let token = c.encode();
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&token)
        .unwrap();
    let s = std::str::from_utf8(&bytes).unwrap();
    let a_pos = s.find("a_field").unwrap();
    let z_pos = s.find("z_field").unwrap();
    assert!(
        a_pos < z_pos,
        "keys must serialize alphabetically (a before z)"
    );
}
