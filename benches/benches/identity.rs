//! Minting and verifying the token on an authenticated request.
//!
//! ```text
//! cargo bench -p benches --bench identity
//! ```
//!
//! `from_token` runs on every authenticated HTTP request, so its cost is paid
//! per request before any database work starts. It is signature verification,
//! which means the curve the deployment picked decides the number, and nothing
//! here compared them.

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use identity::RawIdentity;

const AUDIENCE: &str = "defradb-bench";
const TTL: Duration = Duration::from_secs(3600);

/// One identity per curve the token path supports. BLS is deliberately absent:
/// `new_token` rejects it, so there is no token to measure.
fn identities() -> Vec<(&'static str, RawIdentity)> {
    vec![
        (
            "ed25519",
            RawIdentity::from_private_key(crypto::generate_ed25519().expect("a key"))
                .expect("an identity"),
        ),
        (
            "secp256k1",
            RawIdentity::from_private_key(crypto::generate_secp256k1().expect("a key"))
                .expect("an identity"),
        ),
        (
            "secp256r1",
            RawIdentity::from_private_key(crypto::generate_secp256r1().expect("a key"))
                .expect("an identity"),
        ),
    ]
}

fn mint(c: &mut Criterion) {
    let mut group = c.benchmark_group("identity_new_token");
    for (curve, id) in identities() {
        group.bench_with_input(BenchmarkId::from_parameter(curve), &id, |b, id| {
            b.iter(|| {
                black_box(
                    identity::new_token(black_box(id), TTL, Some(AUDIENCE.to_string()), None)
                        .expect("the token to mint"),
                )
            })
        });
    }
    group.finish();
}

fn verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("identity_from_token");
    for (curve, id) in identities() {
        let token = identity::new_token(&id, TTL, Some(AUDIENCE.to_string()), None)
            .expect("the token to mint");
        group.bench_with_input(BenchmarkId::from_parameter(curve), &token, |b, token| {
            b.iter(|| {
                black_box(identity::from_token(black_box(token)).expect("the token to verify"))
            })
        });
    }
    group.finish();
}

/// The audience and expiry check that follows signature verification on every
/// request, measured on its own so a regression in either is attributable.
fn audience(c: &mut Criterion) {
    let (_, id) = identities().remove(0);
    let token =
        identity::new_token(&id, TTL, Some(AUDIENCE.to_string()), None).expect("the token to mint");
    let verified = identity::from_token(&token).expect("the token to verify");
    let mut group = c.benchmark_group("identity_verify_auth_token");
    group.bench_function("ed25519", |b| {
        b.iter(|| {
            identity::verify_auth_token(black_box(&verified), AUDIENCE)
                .expect("the audience to match")
        })
    });
    group.finish();
}

criterion_group!(benches, mint, verify, audience);
criterion_main!(benches);
