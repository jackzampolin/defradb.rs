use db::merge::peer_identity::*;
use libp2p::identity::secp256k1;
use libp2p::identity::Keypair;
use libp2p::PeerId;

// Known secp256k1 fixture that Go's `crypto.NewPublicKey(...).DID()`
// produces for the same 32-byte private key. Mirrors the constants in
// crates/crypto/tests/go_compat_keys.rs so this test exercises the
// full extraction pipeline (libp2p protobuf → compressed 33 bytes →
// crypto::Secp256k1PublicKey → uncompressed 65 bytes → DID).
const GO_SECP256K1_PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];
const GO_SECP256K1_DID: &str =
    "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";

fn libp2p_keypair_from_go_fixture() -> Keypair {
    let mut sk_bytes = GO_SECP256K1_PRIVATE_KEY;
    let sk = secp256k1::SecretKey::try_from_bytes(&mut sk_bytes).unwrap();
    secp256k1::Keypair::from(sk).into()
}

// Go's defradb/crypto/keys.go:276-282 derives the secp256k1 DID from
// the uncompressed SEC1 public key. `crypto::Secp256k1PublicKey::did`
// already does the equivalent. Peer-identity conversion must produce
// the same DID starting from libp2p's compressed key so Rust and Go
// agree on the DID for the same peer.
#[test]
fn test_secp256k1_peer_to_did_matches_crypto_did() {
    let libp2p_keypair = Keypair::generate_secp256k1();
    let libp2p_pk = libp2p_keypair.public();
    let peer_id = PeerId::from_public_key(&libp2p_pk);

    let did_from_peer_id = peer_id_to_did(&peer_id).expect("secp256k1 peer_id must convert to did");
    let did_from_public_key =
        public_key_to_did(&libp2p_pk).expect("secp256k1 public key must convert to did");

    assert_eq!(
        did_from_peer_id, did_from_public_key,
        "peer_id and public_key conversions must agree"
    );
    // Go produces the uncompressed secp256k1 DID with the base58btc
    // `z7r8` prefix (multicodec 0xe7 + 65-byte uncompressed point).
    assert!(
        did_from_peer_id.as_str().starts_with("did:key:z7r8"),
        "secp256k1 DID must start with did:key:z7r8, got {}",
        did_from_peer_id.as_str()
    );

    // Sanity: parsing the DID back must round-trip to a secp256k1
    // key type with uncompressed SEC1 bytes (65 bytes starting 0x04).
    let (kt, bytes) =
        crypto::parse_did_key(did_from_peer_id.as_str()).expect("DID must round-trip");
    assert_eq!(kt, crypto::KeyType::Secp256k1);
    assert_eq!(bytes.len(), 65);
    assert_eq!(bytes[0], 0x04);
}

// Exercises the full libp2p-side extraction against a fixture that
// matches `crates/crypto/tests/go_compat_keys.rs::test_secp256k1_did_matches_go`.
// Rust must produce the same DID Go produces for the same 32-byte
// private key when routed through libp2p's protobuf encoding.
#[test]
fn test_secp256k1_public_key_to_did_matches_go_fixture() {
    let kp = libp2p_keypair_from_go_fixture();
    let did = public_key_to_did(&kp.public()).expect("secp256k1 DID conversion must succeed");
    assert_eq!(
        did.as_str(),
        GO_SECP256K1_DID,
        "libp2p secp256k1 path must produce the same DID Go produces for the fixture key"
    );
}

#[test]
fn test_secp256k1_peer_id_to_did_matches_go_fixture() {
    let kp = libp2p_keypair_from_go_fixture();
    let peer_id = PeerId::from_public_key(&kp.public());
    let did = peer_id_to_did(&peer_id).expect("secp256k1 DID conversion must succeed");
    assert_eq!(did.as_str(), GO_SECP256K1_DID);
}

#[test]
fn test_secp256k1_peer_to_did_is_deterministic() {
    let libp2p_keypair = Keypair::generate_secp256k1();
    let peer_id = PeerId::from_public_key(&libp2p_keypair.public());

    let did1 = peer_id_to_did(&peer_id).unwrap();
    let did2 = peer_id_to_did(&peer_id).unwrap();
    assert_eq!(did1, did2);
}

#[test]
fn test_secp256k1_mapper_function() {
    let libp2p_keypair = Keypair::generate_secp256k1();
    let peer_id = PeerId::from_public_key(&libp2p_keypair.public());

    let mapper = create_peer_to_did_mapper();
    let did = mapper(&peer_id.to_string()).expect("secp256k1 peer must map to DID");
    assert!(did.as_str().starts_with("did:key:z"));
}

#[test]
fn test_ed25519_peer_to_did() {
    // Generate an Ed25519 keypair
    let keypair = Keypair::generate_ed25519();
    let peer_id = PeerId::from_public_key(&keypair.public());

    // Convert to DID
    let did = peer_id_to_did(&peer_id);

    // Should succeed for Ed25519
    assert!(did.is_ok(), "Ed25519 peer should convert to DID: {:?}", did);
    let did = did.unwrap();
    assert!(did.as_str().starts_with("did:key:z"));
}

#[test]
fn test_public_key_to_did_ed25519() {
    let keypair = Keypair::generate_ed25519();
    let public_key = keypair.public();

    let did = public_key_to_did(&public_key);
    assert!(did.is_ok());
    assert!(did.unwrap().as_str().starts_with("did:key:z"));
}

#[test]
fn test_mapper_function() {
    let keypair = Keypair::generate_ed25519();
    let peer_id = PeerId::from_public_key(&keypair.public());
    let peer_id_str = peer_id.to_string();

    let mapper = create_peer_to_did_mapper();
    let did = mapper(&peer_id_str);

    assert!(did.is_some());
    assert!(did.unwrap().as_str().starts_with("did:key:z"));
}

#[test]
fn test_mapper_invalid_peer_id() {
    let mapper = create_peer_to_did_mapper();
    let did = mapper("invalid-peer-id");

    assert!(did.is_none());
}

#[test]
fn test_deterministic_did() {
    // Same peer should always produce same DID
    let keypair = Keypair::generate_ed25519();
    let peer_id = PeerId::from_public_key(&keypair.public());

    let did1 = peer_id_to_did(&peer_id).unwrap();
    let did2 = peer_id_to_did(&peer_id).unwrap();

    assert_eq!(did1, did2);
}
