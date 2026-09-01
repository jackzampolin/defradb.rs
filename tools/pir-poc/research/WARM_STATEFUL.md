# Warm/stateful common-corpus benchmark

`bench-warm-stateful` replaces the older raw-synthetic SinglePass comparison for
decision-making. It runs stateless Dense XOR and the existing stateful
SinglePass implementation over exactly the same populated snapshot:

- 1,048,576 documents;
- 262,144 distinct tags and exact-MPHF rows;
- four 16-byte compact locators per tag;
- one 96-byte encoded page per tag;
- 25,165,824 table bytes per server;
- two non-colluding servers, both required to answer.

The command is:

```sh
cargo run -p pir-poc --release -- bench-warm-stateful full
```

Do not run timing comparisons while another build or benchmark is consuming the
same CPU or memory bandwidth. `quick` executes each client lifetime once;
`full` executes it three times and reports the median lifetime total.

## What is measured

Every 1, 2, 10, 100, and 1,000-query client lifetime is executed directly.
SinglePass starts each lifetime from a fresh authenticated MPHF load and fresh
mutable preprocessing state. Queries inside a lifetime are sequential state
mutations; they are not a server batch. In particular, the one-query result is
not extrapolated from a warmed 1,000-query run.

The report keeps these quantities separate:

1. global encoded-corpus and exact-MPHF build time;
2. per-client public MPHF metadata load;
3. per-client SinglePass table scan, hint/permutation build, and state bytes;
4. online aggregate server time, co-located wall time, and logical row bytes;
5. online client preparation and completion/show-and-shuffle time;
6. setup and online upload/download bytes;
7. immutable-generation rebuild and client-state refresh semantics.

The amortization matrix crosses 1/2/10/100/1,000 sequential queries per client
with 1/1,000/1,000,000 clients per immutable generation. It exposes server-time
components, client-time components, server egress, client upload, and active
client state independently. It never adds milliseconds to bytes or substitutes
logical payload bytes for physical memory traffic or energy.

## SinglePass setup is a table transfer, not a small hint

The implementation faithfully creates private permutations and parity hints on
the client. Therefore the client must receive and scan the entire authorized
25 MiB locator table for each new immutable generation. The resulting parity
hints are generated locally, so `server_produced_hint_transfer_bytes` is zero.
Calling the table stream a hint would conceal the dominant cold-client cost.

There is no cryptographic server-side preprocessing beyond the common immutable
MPHF table. CPU used to serve the table or metadata (filesystem, TLS, network
stack, retries) is not measured in-process. Required bytes are still charged as
server egress/client download, and the server-time total explicitly excludes
that unmeasured transport CPU.

The comparison assumes every client is authorized to receive the whole locator
projection. If locators for non-result tags must remain hidden from that client,
SinglePass has a different data-disclosure scope and its ratio to Dense is not a
valid production comparison.

## `Q` and query count are different

`partition_count_q` is the SinglePass partition count. The implementation and
construction require `Q >= 2`; the benchmark measures 2, 4, 8, 16, and 32.
Smaller `Q` reduces online indexed reads/responses but increases parity-hint
state. `sequential_queries_per_client` is instead the number of queries served
by one mutable state lifetime and includes a directly measured value of one.

## Generation and update behavior

SinglePass state is bound to the exact 32-byte MPHF generation. Present rows are
fingerprint-verified, absent tags privately retrieve an unrelated row and are
rejected by the 128-bit fingerprint, and stale state is rejected before query
preparation mutates it.

The immutable POC has no incremental update path. A changed dataset requires:

1. rebuilding and publishing the encoded pages and exact MPHF table;
2. authenticating a new generation manifest;
3. making every SinglePass client discard old state, download the new metadata
   and table, and rebuild its hints/permutations.

Each completed SinglePass query mutates hints and permutations. Production must
persist that post-query state atomically. If a request may have reached a server
but completion is ambiguous, the client must recover a committed state or throw
it away; it must not roll back and reuse an observed query state.

## Why there is no three-server SinglePass number

This SinglePass construction has exactly two asymmetric query roles: refresh
and punctured. Recovery and show-and-shuffle consume those two answers. Adding a
third replica of either role may help availability, but it does not create a
third independent share or improve collusion tolerance. A real 3+ server result
needs a different construction and proof, so the report emits a blocked topology
comparison rather than an invented ratio.
