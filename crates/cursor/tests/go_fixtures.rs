//! Cross-compat fixtures: the `token` values in `fixtures/all.json` are produced
//! by Go's `internal/cursor.Encode` (CursorPayload `{d, k(omitempty), o}` →
//! `base64.RawURLEncoding`). These tests assert the Rust codec decodes and
//! encodes byte-for-byte identically, guaranteeing cursor interoperability.

use cursor::Cursor;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    token: String,
    decoded: DecodedFixture,
}

#[derive(Debug, Deserialize)]
struct DecodedFixture {
    d: String,
    #[serde(default)]
    k: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    o: String,
}

fn load_fixtures() -> Vec<Fixture> {
    let raw = include_str!("fixtures/all.json");
    serde_json::from_str(raw).expect("fixtures must be valid JSON")
}

#[test]
fn decode_matches_go() {
    for f in load_fixtures() {
        let decoded =
            Cursor::decode(&f.token).unwrap_or_else(|e| panic!("{}: decode failed: {}", f.name, e));
        assert_eq!(decoded.doc_id, f.decoded.d, "{}: doc_id mismatch", f.name);
        assert_eq!(decoded.keys, f.decoded.k, "{}: keys mismatch", f.name);
        assert_eq!(
            decoded.direction, f.decoded.o,
            "{}: direction mismatch",
            f.name
        );
    }
}

#[test]
fn encode_matches_go_byte_for_byte() {
    for f in load_fixtures() {
        let c = Cursor {
            doc_id: f.decoded.d.clone(),
            keys: f.decoded.k.clone(),
            direction: f.decoded.o.clone(),
        };
        let token = c.encode();
        assert_eq!(
            token, f.token,
            "{}: encoded token does not match Go-produced token byte-for-byte",
            f.name
        );
    }
}
