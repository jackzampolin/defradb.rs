# Production boundary for DefraDB PIR

The production shape should keep PIR as an optional immutable serving index,
not as another DefraDB query executor or storage engine. The POC therefore
builds and serves byte tables and manifests; it does not change CRDT merge,
document storage, query planning, or transaction semantics.

## Existing Defra primitives to reuse

- Successful mutations already publish `events::EventName::Update` after the
  transaction commits. The event carries the collection ID, document ID, CID,
  and serialized block bytes.
- The query layer already implements GraphQL subscriptions by listening to
  update events and re-running a query scoped to the event's document ID and
  CID.
- Blocks are content-addressed and can be verified against their CID.
- An embedded node exposes the event bus and normal query executor without
  requiring a new database engine.

These are sufficient boundaries for both immutable snapshots and a live PIR
sidecar. No PIR code needs to enter a collection's ordinary read path.

## Snapshot path

```text
normal authorized query/export
  -> deterministic projection rows
  -> immutable PIR generation builder
  -> signed manifest + table artifact(s)
  -> replicated PIR serving processes
```

The builder runs once for a sealed generation or public time window. It sorts
keys and values deterministically, pads the configured result cardinality,
builds the chosen keyword-to-row layout, and writes immutable artifacts. A
serving process memory-maps only completed artifacts and atomically switches a
manifest pointer after all replicas have the same generation.

The manifest must commit to:

- collection/schema identity and the exact exported projection;
- source cutoff, generation identifier, and ordered input digest;
- row count, row width, page/cardinality policy, and layout parameters;
- every server artifact digest and the client metadata digest;
- server topology, collusion threshold, required answers, and protocol
  version.

A client sends the generation ID with every share. Replicas must reject stale
or mismatched shares; the client combines only answers for one authenticated
manifest. This prevents a cross-generation XOR from producing plausible but
incorrect bytes.

## Live path

```text
committed Update event
  -> projection/tag extractor
  -> compact-DPF subscription evaluator on each replica
  -> fixed-size answer share
```

The sidecar can subscribe to the existing update bus. The first implementation
may use a normal scoped Defra query to materialize the configured projection
for each event. A later optimization can decode the committed block or expose
an optional projection callback from the mutation path, but that is not needed
to prove the protocol.

Subscription state is generation-scoped and replicated independently to each
server. Registration, renewal, cancellation, event batching, padding, and
delivery cadence are all observable and must have an explicit leakage policy.
Compact DPF hides the predicate from each of two non-colluding replicas; it
does not hide collection, registration time, event rate, or response timing.

## What a result contains

The benchmark must distinguish four products:

1. locator only (CID/document ID);
2. bounded inline projection;
3. a fixed-size capsule containing locators plus selected fields;
4. locator lookup followed by private batched retrieval from a document table.

Inline projection is the simplest production default when the useful payload
has a small configured maximum. It makes one private retrieval end-to-end and
avoids leaking which returned CID is fetched next. Large or variable documents
use a second immutable table keyed by a compact document ordinal; the client
must retrieve a padded batch so cardinality and the chosen locator are not
revealed. Ordinary public CID fetches are not an end-to-end private result.

## Authorization boundary

PIR hides a query from a server; it does not grant access. A single shared PIR
artifact is appropriate only for data visible to every client allowed to query
that artifact. Private data requires a separately authorized cohort artifact,
encrypted projection values with cohort key distribution, or an additional
private authorization protocol. Building one global plaintext table and
relying on the hidden selector would bypass DefraDB ACP.

For encrypted projections, encryption happens before table construction under
an application/cohort key the PIR server does not possess. PIR returns the
ciphertext bytes and the authorized client decrypts them. A signed manifest
authenticates the committed generation/artifact, but it does not authenticate
an arbitrary XOR answer produced by a malicious replica. Projection ciphertext
needs AEAD (with generation/ordinal associated data), or the PIR response needs
a separately proven committed/verifiable mechanism. CIDs and 128-bit row
fingerprints are useful semi-honest corruption checks, not malicious-server
proofs.

## Failure and deployment rules

- Replicas are separate trust/administrative domains; multiple processes on
  one operator are only a performance test, not a non-collusion claim.
- The main replicated-XOR lane provides query privacy against any `n - 1`
  colluding semi-honest servers and requires all `n` answers. It does not give
  availability or correctness against malicious replicas.
- Signed manifests bind the expected generation and artifacts; embedded
  fingerprints probabilistically reject many malformed or stale rows in the
  semi-honest POC. They do not authenticate arbitrary malicious answers.
  Robust verification/recovery requires AEAD/commitments plus a separately
  designed coded or verifiable protocol and must not be implied by adding a
  third replica.
- Builds are copy-on-write. A failed builder never mutates the currently
  served generation.
- Persisted MPHF and subset-XOR artifacts need stable, bounds-checked formats;
  the POC's same-build `epserde` representation is not the production wire
  format.

## Serving admission is still an integration gate

The selected HTTP/OHTTP service now enforces authenticated selector, batch,
table, response, transient-memory, concurrency and subscription limits, and it
rejects duplicate subscription identifiers. Clients and the OHTTP relay also
stream responses through local byte ceilings. Historical research servers do
not all share this boundary, and the POC still lacks principal-aware rate
limits, bounded queue dwell, cancellation and production metrics. Preserve the
selected service's fail-before-evaluation checks when extracting production
crates, then add those operational controls at the serving edge.

## Minimal integration sequence

1. Freeze this POC as the decision/evidence archive.
2. Add a read-only snapshot-export adapter using the normal authorized embedded
   query API; make its deterministic output digest the builder input.
3. Replace JSON-loaded table images with a versioned, bounded binary artifact
   that servers can memory-map and publish atomically.
4. Add an optional sidecar listener on `EventName::Update` for live Compact-DPF
   evaluation, using the existing research demonstration as the seam test.
5. Extract only the stable manifest, client combine/verification and selected
   serving APIs into small crates. Do not move experimental layouts into core
   Defra crates.
