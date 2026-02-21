# Finding: ECIES Does Not Reject X25519 Low-Order Public Keys

**Stream**: 01 - Cryptographic Inventory
**Session**: 3 - Encryption & ECIES Correctness
**Severity**: LOW-MEDIUM
**Category**: ECIES / Go Compatibility / Defense-in-Depth
**Status**: NEW

## Summary

The Rust ECIES implementation accepts X25519 low-order public keys (including the all-zeros point) without error, producing predictable shared secrets. Go's `crypto/ecdh` package explicitly rejects ECDH operations that produce all-zero shared secrets. This is a behavioral divergence that could cause interoperability issues — Go nodes reject ciphertexts that Rust nodes accept.

## Evidence

### Rust: Accepts All-Zeros Shared Secret

`crates/crypto/src/encryption/ecies.rs:122` (encrypt) and `:227` (decrypt):

```rust
let shared_secret = ephemeral_private.diffie_hellman(public_key);
```

`x25519-dalek 2.0.1`'s `diffie_hellman()` returns any result, including all-zeros, without validation:

```rust
// x25519-dalek source (simplified)
pub fn diffie_hellman(self, their_public: &PublicKey) -> SharedSecret {
    SharedSecret(self.0.to_montgomery().mul_clamped(their_public.0).to_bytes())
}
```

### Go: Rejects All-Zeros Shared Secret

`crypto/ecies.go:139`:

```go
sharedSecret, err := ourPrivateKey.ECDH(publicKey)
```

Go's `crypto/ecdh` package (since Go 1.20) explicitly checks: "if the result is the all-zero value, ECDH returns an error." This rejects ECDH operations with low-order public keys that produce degenerate shared secrets.

### Test Confirms Rust Accepts Zero-Point

`crates/crypto/tests/ecies_tests.rs:292-308`:

```rust
fn test_encrypt_with_weak_public_key() {
    let zero_pub = PublicKey::from([0u8; 32]);
    let ciphertext = encrypt_ecies(plaintext, &zero_pub, options_enc).unwrap();  // Succeeds
    // ...
}
```

Encryption to the all-zeros public key succeeds in Rust. In Go, this would fail during the ECDH step.

## Why This Matters

### Small-Subgroup Attack Vector

The all-zeros X25519 public key represents the identity element on Curve25519. For any private key `k`, the ECDH result `k * identity = identity = [0; 32]`. This means:

1. The shared secret is always `[0; 32]`, regardless of the recipient's private key
2. HKDF derives predictable AES and HMAC keys from this zero secret
3. Anyone who knows the shared secret is zero can compute these derived keys
4. The HMAC tag is forgeable, and the ciphertext is decryptable by anyone

In the **decrypt path**, an attacker could construct ciphertext with a low-order ephemeral public key:

| Step | Attacker Action |
|------|----------------|
| 1 | Set ephemeral public key in ciphertext to `[0; 32]` |
| 2 | Victim computes ECDH: `victim_private * [0; 32] = [0; 32]` |
| 3 | HKDF derives predictable keys from zero shared secret |
| 4 | Attacker (knowing the keys) can construct valid HMAC |
| 5 | AES-GCM decryption succeeds with attacker-controlled plaintext |

### Why Impact is LOW-MEDIUM (Not Higher)

ECIES does not provide sender authentication. The scheme only guarantees confidentiality (the recipient can read the message) and integrity (the message wasn't tampered with). An attacker can always encrypt any message to the victim using the victim's public key — the low-order key attack doesn't give them additional capabilities beyond what they already have as a sender.

The residual concern is:
- **Protocol-level trust**: If a higher-level protocol trusts "ECIES-decrypted data" as implying a specific sender, the zero-point attack bypasses this assumption. ECIES provides no such guarantee, but protocol designers may incorrectly assume it.
- **Go interop divergence**: A Go node would reject this ciphertext (ECDH error); a Rust node would accept it. This creates inconsistent behavior in a mixed Go/Rust network.

### Other Low-Order Points

Besides the all-zeros point, Curve25519 has 7 other small-order points. X25519's "clamping" (clearing bottom 3 bits of private key) ensures `8 * small_order_point = identity`, so all small-subgroup shared secrets collapse to all-zeros. The same validation gap applies to all of them.

## Affected Code

- **Encrypt**: `crates/crypto/src/encryption/ecies.rs:122` — `diffie_hellman()` result unchecked
- **Decrypt**: `crates/crypto/src/encryption/ecies.rs:227` — same
- **Key construction**: `PublicKey::from([u8; 32])` at `:214` accepts any 32 bytes without validation

## Remediation

Add a post-ECDH check matching Go's behavior:

```rust
let shared_secret = ephemeral_private.diffie_hellman(public_key);

// Reject degenerate shared secrets (matches Go's crypto/ecdh behavior)
if shared_secret.as_bytes().iter().all(|&b| b == 0) {
    return Err(crypto_error("ECDH produced all-zero shared secret (low-order public key)"));
}
```

Apply in both `encrypt_ecies` (line 122) and `decrypt_ecies` (line 227). This aligns with Go's behavior and provides defense-in-depth against small-subgroup attacks.
