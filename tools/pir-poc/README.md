# DefraDB PIR POC

This sidecar demonstrates three selected private-query paths without changing
DefraDB query execution or storage:

1. a live Shieldd nullifier generation and fixed 2,008-byte witness;
2. an encrypted tag projection with a fixed result class;
3. a Shinzo wallet-event subscription using two-party Compact DPF.

Strict Dense XOR and the 100-decoy baseline share the same immutable serving
rows. The decoy client decodes only its known target row and drops the other 99.
The complete protocol explanation, client/server benchmark table, privacy
comparison and two-versus-three-server discussion live in
[USE_CASES.md](USE_CASES.md). It is the single authoritative design document;
this README is only the operating guide.

The end-to-end threat model and research-backed privacy layers outside PIR—
origin hiding, timing resistance, anonymous admission, live delivery and
private transaction broadcast—are documented in [PRIVACY.md](PRIVACY.md).

## Commands

The default binary deliberately has five top-level commands:

```text
pir-poc demo
pir-poc build INPUT_JSON OUTPUT_ROOT
pir-poc serve REPLICA_STORE BIND_ADDRESS
pir-poc query ...
pir-poc benchmark [quick|full]
```

Run the complete in-memory HTTP demo:

```bash
cargo run -p pir-poc --release -- demo
```

The demo starts two replicas, verifies a reconstructed nullifier witness
against a canonical Shieldd Poseidon root, authenticates/decrypts an encrypted
tag projection, executes a 100-decoy lookup while processing one row, and
registers/evaluates matching and missing Shinzo Compact-DPF subscriptions. It
then repeats all three private paths through two independent RFC 9458 OHTTP
relay/gateway paths. Its final table measures cold setup and 11-query p50
verified latency for a visible direct lookup, direct PIR, and OHTTP PIR. A real
Tor lane is included only when configured:

```bash
PIR_POC_TOR_SOCKS_URL=socks5h://127.0.0.1:9050 \
  cargo run -p pir-poc --release -- demo
```

The Tor lane deliberately has no simulated fallback. For a meaningful run,
deploy the OHTTP relays at remote HTTPS addresses reachable through Tor; the
loopback demo addresses may be rejected by an exit path. The SOCKS POC proves
transport interchangeability, not circuit isolation; a native Arti integration
should use a separate isolation context for each replica path.

## Build and serve an immutable generation

`build`, `serve`, and `query` obtain the operator authentication key from an
out-of-band environment variable:

```bash
export PIR_POC_OPERATOR_KEY_HEX=<64 hex characters>
cargo run -p pir-poc --release -- build input.json ./pir-store
cargo run -p pir-poc --release -- serve ./pir-store/replica-0 127.0.0.1:8080
cargo run -p pir-poc --release -- serve ./pir-store/replica-1 127.0.0.1:8081
```

Tag query clients also need the 32-byte projection AEAD key through
`PIR_POC_PROJECTION_KEY_HEX`. This key belongs to the wallet/application and is
never sent to a PIR replica, relay, or OHTTP gateway.

The input shape is documented by
[selected-input.schema.json](selected-input.schema.json). The builder writes
two immutable, party-specific replica directories. It hashes every table and
safe ordinal directory, MACs the generation manifest, fsyncs files, and renames
the completed temporary directory into place. Existing generations are never
overwritten.

Strict queries:

```bash
cargo run -p pir-poc --release -- query strict nullifier <32-byte-hex> \
  http://127.0.0.1:8080 http://127.0.0.1:8081

cargo run -p pir-poc --release -- query strict tag <tag-base64> \
  http://127.0.0.1:8080 http://127.0.0.1:8081
```

Candidate-set query using one server and a JSON array containing exactly 100
encoded candidate keys:

```bash
cargo run -p pir-poc --release -- query decoy tag <target-base64> \
  candidates.json http://127.0.0.1:8080
```

Shinzo registration followed by one event evaluation:

```bash
cargo run -p pir-poc --release -- query shinzo 1234 1234 \
  http://127.0.0.1:8080 http://127.0.0.1:8081
```

## Security and admission boundaries

- Dense XOR requires every configured replica answer and hides the target if at
  least one replica does not collude.
- Compact DPF is computationally private under its construction and AES PRG and
  is two-party in this implementation.
- Decoys provide candidate-set privacy only. The server sees all candidates,
  cardinalities, popularity and longitudinal intersections.
- The stable client ordinal directory is safe to parse but exposes populated
  key digests to dictionary attacks. It replaces unsafe PtrHash/epserde metadata
  on the default serving path.
- The generation height, root, table shapes, fixed result schedules and limits
  are authenticated as one body. Clients reject divergent replicas and stale
  or internally inconsistent generations.
- Query, response, batch, metadata, table, transient-memory, in-flight and
  subscription limits are enforced before expensive evaluation/allocation.
- Default query APIs recompute Shieldd's 20-level, 4-ary Poseidon path against
  the authenticated root and authenticate tag projections with AES-256-GCM.
  AEAD associated data binds generation height/root, tag and result slot.
- The authenticated root is only as trustworthy as its out-of-band source. The
  demo uses an operator MAC; production should pin the root via Shieldd
  consensus/light-client state rather than trusting the queried providers.
- OHTTP hides the wallet address from a PIR gateway and the PIR share from its
  relay. It does not hide timing from a global observer, and relay/gateway
  collusion recovers the client-to-query relationship. The loopback demo uses
  plain HTTP; deployment must use HTTPS on both hops and independent operators.
- The OHTTP client uses a small transport interface with direct and Tor SOCKS
  backends. Tor configuration requires `socks5h://` so relay DNS is not leaked
  to the wallet's local resolver.

## Research archive

Historical Dense/DPF/Fuse/Ribbon/SinglePass/computational-PIR experiments are
not compiled into the normal product-shaped binary. To inspect or rerun them:

```bash
cargo run -p pir-poc --release --features research -- research <benchmark> quick
```

See [research/README.md](research/README.md). These experiments remain evidence,
not additional runtime protocol choices.
