# identity new Prints Private Key Material to stdout

- **Severity**: Medium
- **Category**: Credential Safety
- **Status**: Confirmed — Previously found (finding 27), additional context here

## Summary

When `identity new` is called WITHOUT `--name` (no keyring storage), both the private key hex and public key hex are printed to stdout. This is the Go-compatible default behavior but creates risk of key material being captured in terminal scrollback, piped to files, or logged by terminal recording software.

## Affected Files

- `crates/cli/src/commands/identity.rs:162-178` (IdentityNewArgs::execute, unnamed mode)

## Details

```rust
// identity.rs:162-178
} else {
    let private_key_hex = hex::encode(&raw_bytes);
    let public_key_hex = hex::encode(identity.public_key_bytes());
    let key_type_str = identity.identity_key_type().to_string();
    match self.output.to_lowercase().as_str() {
        "text" => {
            println!("Private key: {}", private_key_hex);  // ← private key on stdout
            println!("DID: {}", did);
        }
        _ => {
            let output = serde_json::json!({
                "PrivateKey": private_key_hex,  // ← private key in JSON on stdout
                "PublicKey": public_key_hex,
                "DID": did.to_string(),
                "KeyType": key_type_str,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
    }
}
```

When `--name` IS provided, the private key is stored in the keyring and only the DID is printed (unless `--output-key` is explicitly requested):
```rust
// identity.rs:147-159
if self.output_key {
    let jwk = build_jwk(identity.identity_key_type(), &raw_bytes)?;
    print_jwk(&jwk, &self.output);
} else {
    // Only DID printed — safe
    println!("DID: {}", did);
}
```

**Also**: `keyring export` prints raw key hex to stdout:
```rust
// keyring_cmd.rs:186
println!("{}", hex_encode(&key));
```

This is expected behavior for an export command, but worth noting.

**Also**: `identity export` prints private key material as JWK (including `d` parameter):
```rust
// identity.rs:193-196 — calls build_jwk which includes "d" field
```

## Remediation

1. Output private key to stderr instead of stdout (allows safe piping):
   ```rust
   eprintln!("Private key: {}", private_key_hex);
   println!("DID: {}", did);
   ```

2. Or require `--output-key` flag for unnamed mode too, defaulting to DID-only output.

3. This is Go-compatible behavior, so changes should be coordinated.

## Test Gap

No test validates stdout content of `identity new`. Tests exist for key generation but not output format.
