//! The cryptographic primitives on the request and replication paths.
//!
//! ```text
//! cargo bench -p benches --bench crypto
//! ```
//!
//! Nothing here was measured before, and the curve a deployment picks decides
//! several of these numbers by an order of magnitude. Signing runs on every
//! authored commit, verification on every inbound block, AES on every
//! encrypted field, and ECIES on every key handed to a peer.
//!
//! Sizes are swept because these are byte-rate operations: a figure quoted for
//! one payload size says nothing about another.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crypto::{
    decrypt_aes, decrypt_ecies, encrypt_aes, encrypt_ecies, EciesOptionsBuilder, PrivateKey,
};

const SIZES: [usize; 4] = [64, 1024, 16 * 1024, 256 * 1024];
const KEY: [u8; 32] = [0x5a; 32];

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn aes(c: &mut Criterion) {
    let mut group = c.benchmark_group("crypto_aes");
    for size in SIZES {
        let plaintext = payload(size);
        let (ciphertext, nonce) =
            encrypt_aes(&plaintext, &KEY, b"", false).expect("the payload to encrypt");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("encrypt", size), &plaintext, |b, data| {
            b.iter(|| black_box(encrypt_aes(black_box(data), &KEY, b"", false).expect("encrypt")))
        });
        group.bench_with_input(
            BenchmarkId::new("decrypt", size),
            &(ciphertext, nonce),
            |b, (ciphertext, nonce)| {
                b.iter(|| {
                    black_box(
                        decrypt_aes(Some(nonce), black_box(ciphertext), &KEY, b"")
                            .expect("decrypt"),
                    )
                })
            },
        );
    }
    group.finish();
}

/// What it costs to seal a key for one recipient. Every document encryption
/// key handed to a peer goes through this.
fn ecies(c: &mut Criterion) {
    let secret = crypto::generate_x25519().expect("an x25519 secret");
    let public = x25519_dalek::PublicKey::from(&secret);
    // The ephemeral public key travels with the ciphertext, which is how a
    // sealed key actually reaches a peer: without it the recipient has nothing
    // to derive the shared secret from.
    let options = || {
        EciesOptionsBuilder::default()
            .prepend_public_key(true)
            .build()
    };
    let mut group = c.benchmark_group("crypto_ecies");
    for size in [32usize, 1024] {
        let plaintext = payload(size);
        let sealed = encrypt_ecies(&plaintext, &public, options()).expect("the payload to seal");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("seal", size), &plaintext, |b, data| {
            b.iter(|| black_box(encrypt_ecies(black_box(data), &public, options()).expect("seal")))
        });
        group.bench_with_input(BenchmarkId::new("open", size), &sealed, |b, sealed| {
            b.iter(|| {
                black_box(decrypt_ecies(black_box(sealed), &secret, options()).expect("open"))
            })
        });
    }
    group.finish();
}

/// Signing and verification across every curve the node supports. BLS signs
/// but is verified through the threshold path, so only its signing side is
/// comparable here.
fn signatures(c: &mut Criterion) {
    let keys: Vec<(&str, Box<dyn PrivateKey>)> = vec![
        (
            "ed25519",
            Box::new(crypto::generate_ed25519().expect("a key")),
        ),
        (
            "secp256k1",
            Box::new(crypto::generate_secp256k1().expect("a key")),
        ),
        (
            "secp256r1",
            Box::new(crypto::generate_secp256r1().expect("a key")),
        ),
    ];
    let message = payload(256);

    let mut signing = c.benchmark_group("crypto_sign");
    for (curve, key) in &keys {
        signing.bench_with_input(BenchmarkId::from_parameter(curve), key, |b, key| {
            b.iter(|| black_box(key.sign(black_box(&message)).expect("the signature")))
        });
    }
    signing.finish();

    let mut verifying = c.benchmark_group("crypto_verify");
    for (curve, key) in &keys {
        let signature = key.sign(&message).expect("the signature");
        verifying.bench_with_input(BenchmarkId::from_parameter(curve), key, |b, key| {
            b.iter(|| {
                black_box(
                    key.public_key()
                        .verify(black_box(&message), &signature)
                        .expect("the signature to verify"),
                )
            })
        });
    }
    verifying.finish();
}

/// Content addressing. Every block and every document id is hashed, so this is
/// the most-called function in the suite.
fn hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("crypto_sha256");
    for size in SIZES {
        let data = payload(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| black_box(crypto::sha256(black_box(data))))
        });
    }
    group.finish();
}

criterion_group!(benches, aes, ecies, signatures, hashing);
criterion_main!(benches);
