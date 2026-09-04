# DefraDB PIR POC

This sidecar demonstrates three selected private-query paths without changing
DefraDB query execution or storage:

1. a live Shieldd nullifier generation and fixed 2,008-byte witness;
2. an encrypted tag projection with a fixed result class;
3. a Shinzo wallet-event subscription: immediate two-party Compact DPF in the
   served demo, with packed-presence Dense as the measured block/epoch
   production direction.

Strict Dense XOR and the visible-candidate baseline share the same immutable
serving rows. The benchmark uses 100 candidates; the decoy client decodes only
its known target row and drops the others.

## Documentation map

The documentation has one owner for each question:

| Read this | For |
|---|---|
| [PROTOCOLS.md](PROTOCOLS.md) | What each protocol does, its trust assumptions and tradeoffs |
| [USE_CASES.md](USE_CASES.md) | Each application's problem, request, result and privacy need |
| [DECISIONS.md](DECISIONS.md) | Filterable versus full-state requests, recommendations, limits and server overhead versus decoys |
| [BENCHMARKS.md](BENCHMARKS.md) | Selected-protocol benchmarks first; alternative protocols and conditional fallbacks second |
| [ROADMAP.md](ROADMAP.md) | Remaining implementation work and future research triggers |
| [PRODUCTION.md](PRODUCTION.md) | The minimal DefraDB export/event adapter, artifact, authorization, and deployment boundary |
| [PRIVACY.md](PRIVACY.md) | OHTTP, Tor, timing, admission, live delivery, and write-path privacy |
| [PORTABLE_READINESS.md](PORTABLE_READINESS.md) | Client portability budgets and remaining production gates |
| [research/README.md](research/README.md) | Historical protocol comparisons, GPU/CPU artifacts, benchmark ledgers, and reproduction commands |

This README is only the operating guide. Research documents are evidence, not
additional serving choices.

For a team walkthrough: **PROTOCOLS → USE_CASES → DECISIONS**.
Open BENCHMARKS only when supporting data is needed.

## Commands

The default binary deliberately has eight top-level commands:

```text
pir-poc demo
pir-poc use-cases [mizu|shinzo|defra]
pir-poc encrypted-search [ROWS<=1000000]
pir-poc build INPUT_JSON OUTPUT_ROOT
pir-poc serve REPLICA_STORE BIND_ADDRESS
pir-poc bucket shinzo address|topic0 HEX_VALUE [BUCKET_COUNT]
pir-poc query ...
pir-poc benchmark [quick|full]
```

Run the application-shaped fixtures without starting servers:

```bash
cargo run -p pir-poc --release -- use-cases
cargo run -p pir-poc --release -- use-cases mizu
```

The fixtures use the same Dense XOR and Compact-DPF code as the served POC.
These 256-row tables demonstrate correctness; the research CUDA runner adds
the packed-presence epoch protocol. See [BENCHMARKS.md](BENCHMARKS.md) for
measurements and [DECISIONS.md](DECISIONS.md) for protocol choices.

Run the separate blind-token search experiment (not strict PIR):

```bash
cargo run -p pir-poc --release -- encrypted-search 1000
cargo run -p pir-poc --release -- encrypted-search 1000000
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
relay/gateway paths. Its final table measures setup, first-query, p50 and p95
verified latency over 11 queries. OHTTP rows also report aggregate encrypted
upload/download per query across both replicas. A real Tor lane is included
only when all three transport variables are configured:

```bash
PIR_POC_OHTTP_RELAY_BINDS=127.0.0.1:19080,127.0.0.1:19081 \
PIR_POC_TOR_SOCKS_URL=socks5h://127.0.0.1:19050 \
PIR_POC_TOR_RELAY_URLS=http://REPLICA_0.onion,http://REPLICA_1.onion \
  cargo run -p pir-poc --release -- demo
```

For a local onion transport test, map two v3 onion services to the two fixed
relay binds. The Tor SOCKS listener must enable authentication isolation:

```text
SocksPort 127.0.0.1:19050 IsolateSOCKSAuth
HiddenServiceDir /path/to/onion-replica-0
HiddenServiceVersion 3
HiddenServicePort 80 127.0.0.1:19080
HiddenServiceDir /path/to/onion-replica-1
HiddenServiceVersion 3
HiddenServicePort 80 127.0.0.1:19081
```

The client supplies a distinct SOCKS username for each PIR replica, requesting
separate Tor circuit contexts. The Tor lane deliberately has no simulated
fallback. Two onion services on one Tor process validate the transport but do
not establish operator non-collusion; production needs independently operated
relay/gateway/replica paths.

## Build and serve the bundled demo generation

The JSON schema intentionally bundles the nullifier, encrypted-tag, and Shinzo
fixtures so one command can exercise every POC endpoint. It is not the future
DefraDB adapter contract. Production should publish one artifact per bounded
query class through the boundary in [PRODUCTION.md](PRODUCTION.md), rather than
teaching DefraDB about this demo bundle.

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

Candidate-set query using one server and a JSON array containing the target
exactly once. The default authenticated limit is 100 encoded candidate keys:

```bash
cargo run -p pir-poc --release -- query decoy tag <target-base64> \
  candidates.json http://127.0.0.1:8080
```

Shinzo registration followed by one event evaluation:

```bash
cargo run -p pir-poc --release -- query shinzo 1234 1234 \
  http://127.0.0.1:8080 http://127.0.0.1:8081
```

For a real Shinzo event stream, set `SHINZO_PIR_INGEST_TOKEN` on each sidecar
and enable `pir` in `shinzo-host-client/config/config.yaml`. The host posts
authenticated public event batches to `/v1/shinzo/events`; each replica stores
its own result shares for wallet retrieval through `/v1/shinzo/poll`. Ingest is
deliberately excluded from OHTTP because it is an authenticated operator path;
wallet registration and polling can use OHTTP.

Register once, then poll using the returned unguessable subscription ID and
cursor. Each poll advances only through the common ordered prefix available at
both replicas, so a temporarily lagging operator cannot make the client skip an
uncombined share:

```bash
TARGET_BUCKET=$(cargo run -q -p pir-poc --release -- bucket shinzo address \
  0xA0b86991c6218b36c1d19D4a2E9Eb0cE3606eB48 | jq -r .bucket)

cargo run -p pir-poc --release -- query shinzo-register "$TARGET_BUCKET" \
  http://127.0.0.1:8080 http://127.0.0.1:8081

cargo run -p pir-poc --release -- query shinzo-poll \
  <subscription-id> <cursor> \
  http://127.0.0.1:8080 http://127.0.0.1:8081
```

## Security and admission boundaries

- Dense XOR requires every configured replica answer and hides the target if at
  least one replica does not collude.
- Compact DPF is computationally private under its construction and AES PRG and
  is two-party in this implementation.
- Packed-presence Dense uses the same information-theoretic XOR sharing as
  snapshot Dense, extends to three or more replicas, and deliberately reveals
  the fixed public epoch cadence.
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
- HTTP and OHTTP clients and relays stream responses through local size limits;
  they do not trust `Content-Length` as their only memory bound.
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
