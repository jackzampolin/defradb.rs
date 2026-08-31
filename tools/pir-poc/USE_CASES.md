# Selected PIR use cases

This is the authoritative POC decision record. Historical protocol exploration
is archived in `COMPARISON.md`, `EXPLORATION.md`, and `research/`.

Application-shaped Mizu, Shinzo and generic DefraDB examples are implemented in
the [use-case gallery](USE_CASE_GALLERY.md). The gallery reuses the protocols
selected here and does not add another serving architecture.

The selection objective is privacy first, then minimum aggregate server work
among protocols that meet that privacy requirement. Client CPU, upload,
download, storage, build/update work and availability remain separate metrics.
The 100-candidate path is retained as a measured control and an explicit last
resort; its lower server time must not promote it above strict PIR.

## Privacy-first protocol decision ladder

There is no honest total order across cold snapshots, warm stateful clients and
live subscriptions. First classify the query, then start at the top of that
ladder. Moving down requires documenting why every higher strict option fails.
Visible decoys are last in every ladder.

| Rank | Cold snapshot | Select it when | Why the next choice is lower |
|---:|---|---|---|
| 1 | Exact-MPHF/Fuse table + replicated Dense XOR | Two or more independent operators exist and `N/8` upload/server fits | Lowest measured aggregate strict server work, simplest construction, no full client preload, and it extends to 3+ replicas |
| 2 | GPU InsPIRe | Only one operator is available, or a computational single-server trust model is required | More server and client work than Dense locally, but removes the non-collusion assumption and avoids a database-sized client hint |
| 3 | GPU-DPF | Exactly two operators exist, compact upload is mandatory, and a large ready batch amortizes evaluation | Excellent 4,160 B upload, but batch-1 aggregate server work was 71x Dense and it does not extend naturally beyond two servers |
| 4 | Blind exact encrypted search, only with split trust | The independent index provider cannot map tokens to plaintext and equality/access leakage is accepted | It is not PIR: repeats, access and update correlations remain visible |
| 5 | 100 indexed decoys | All strict paths miss the deployment budget and candidate-set leakage is explicitly accepted | The server sees all candidates and can intersect repeated sets; this is a privacy downgrade, not a PIR optimization |

Ranks 2 and 3 are a workload fork rather than a dogmatic order. InsPIRe wins
the measured cold batch-1 fallback (32.21 ms versus 437.73 ms); GPU-DPF moves
ahead when exactly two operators can queue a large batch (13.74 ms versus 18.86
ms at batch 32) and its compact upload is important.

For a warm client repeatedly reading one immutable generation, **SinglePass is
rank 1**: its generation download and mutable state are amortized over many
queries. Dense becomes rank 2 when the client cannot preload, persist state, or
complete SinglePass's atomic exactly-two-server update. InsPIRe, batched
GPU-DPF, split-trust encrypted search and finally decoys follow under the same
conditions as the cold ladder.

For live presence, **packed-presence Dense per public block/epoch is rank 1**.
It ingests each event once and answers each subscriber once per epoch, works
with 2, 3 or more replicas, and was the lowest-work strict live design. Immediate
Compact DPF is rank 2 only when a documented sub-epoch SLA makes batching
unacceptable; its work grows with events times subscribers and it requires
exactly two parties. Visible subscriptions/decoys are last.

These ranks compare privacy protocols. Data shaping happens before them: use a
public authorization-equivalent collection, generation, block range or epoch
to keep an artifact bounded; use exact ordinals/MPHF when all populated keys
can safely be enumerated and Fuse otherwise. A two-stage lookup helps only when
stage one selects a substantially smaller padded table and stage two does not
reveal a secret partition. Merely returning a secret partition ID and reading
it directly leaks that partition; privately selecting it again can restore the
original global scan.

For snapshot data layout, avoid the dominated cuckoo layout. Finite-differences
PIR also remains a research result rather than a production rung: it beat CPU
Dense on one 262K-row case, but charged 8x storage, 5.36 MiB download, exactly
two servers, and has no validated large/GPU result. Path ORAM was excluded
because it adds persistent client state and logarithmic interactive reads to
solve the broader access-sequence problem. TEE+ORAM was excluded because its
hardware trust, deployment and side-channel burden outweigh this POC.
ChalametPIR was excluded from the cold ladder because its stateful client hint
and query work are the wrong scaling direction for a cold/mobile-capable client;
SinglePass is stronger for the warm immutable case, while InsPIRe is the tested
single-server cold candidate.

“High entropy” means a key is computationally hard to guess before seeing it,
for example a random 256-bit transaction hash, document ID, or nullifier. It
does **not** mean the table has many rows or the key has many matches. High-
entropy one-shot keys make plausible decoys and blind tokens harder to rank;
low-bit routing prefixes, enum values, contract addresses and popular tags are
guessable or frequency-identifiable even when their encoded token looks random.

### Benchmark anchors for the ladder

The final same-card snapshot comparison uses five alternating fresh processes
on an RTX 2070 SUPER. Every protocol read the same deterministic 120 useful
bytes/row and every answer was reconstructed:

| Physical table | Batch | Dense XOR, 2 servers | GPU-DPF, 2 servers | InsPIRe GPU, 1 server | 100 visible candidates |
|---:|---:|---:|---:|---:|---:|
| 1 GiB / 8.39M rows | 1 | **6.17 ms** | 437.73 ms | 32.21 ms | 0.01138 ms |
| 1 GiB / 8.39M rows | 32 | **6.14 ms** | 13.74 ms | 18.86 ms | 0.01138 ms |
| 4 GiB / 33.55M rows | 1 | **23.07 ms** | 1,667.08 ms | capacity-blocked | 0.01251 ms |
| 4 GiB / 33.55M rows | 128 | **23.48 ms** | 28.84 ms | capacity-blocked | 0.01251 ms |

The three strict columns are same-card GPU measurements. The visible column is
the separate same-host CPU point-read control: its exact file-backed address
space was mapped, but only the scheduled random pages were made resident. It
is a warm indexed-read baseline, not a cold-storage or same-kernel result.

Dense's batch-1 client generated 2 MiB of shares in 2.68 ms; GPU-DPF generated
4,160 B in 0.084 ms; InsPIRe generated 379,904 B in 47.48 ms. Thus Dense is
the measured server-work winner, not automatically the mobile-network winner.
The visible path is about 542x less server time at 1 GiB and 1,844x less at
4 GiB because it reads 100 rows rather than privately traversing the table. It
does not provide the same privacy.

On the same Ryzen CPU and 1 GiB corpus, two-replica Dense versus pinned Poulpy
InsPIRe2 server wall time was 115.90 versus 415.10 ms at batch 1, 49.11 versus
396.22 ms at batch 8, and 67.59 versus 224.38 ms at batch 32. CPU InsPIRe also
used 5.71--6.87 GiB peak RSS and 30.5--36.1 s offline preprocessing. It is not
the recommended edge-server lane.

### Quantitative acceptance envelopes

The use-case decisions below use the following explicit cost envelopes. `M`
is a directly executed use-case measurement. `P` is a bandwidth projection
from the repeated RTX 2070 SUPER Dense result: 6.17 ms aggregate server work
per resident GiB scanned and 2.68 ms client query generation per 2 MiB aggregate
selector upload. Projections exclude transport, OHTTP, queues, storage faults
and kernel-launch floors. The assumed row counts are conservative upper bounds;
fewer populated key/pages cost less.

| Snapshot use case | Production query class | Strict upload / download | Strict server / client CPU | 100-decoy upload / download | Decoy server / client CPU | Strict server / decoy | Acceptance |
|---|---|---:|---:|---:|---:|---:|---|
| Mizu note recovery | At most 320K populated routing pages in 32 blocks | 80.0 KB / 1.61 KB | **~1.48 / ~0.11 ms `P`** | 3.10 KB / 80.4 KB | 0.0284 / 0.0042 ms `M` | ~52x slower | About 1.45 ms extra server work; total wire is nearly equal (81.6 versus 83.5 KB) |
| Mizu active nullifier witness | 1.05M-leaf active generation | 1.082 MB / 116.7 KB | **34.45 / 22.29 ms `M`** | 3.20 KB / 200.8 KB | 0.141 / 0.00010 ms `M` | 244x slower | Acceptable for occasional proof preparation, not a per-event path; strict saves 84 KB response and hides the candidate set |
| Shinzo historical logs | At most 320K populated pages in 32 blocks | 80.0 KB / 1.10 KB | **~1.01 / ~0.11 ms `P`** | 3.61 KB / 54.8 KB | 0.0264 / 0.0042 ms `M` | ~38x slower | Roughly 1 ms absolute server cost for private address/topic; one-block queries are nearly free |
| Shinzo transaction receipt | At most 10K receipts in one block | 2.50 KB / 368 B | **~0.011 / <0.01 ms `P`** | 3.79 KB / 18.4 KB | 0.0257 / 0.0039 ms `M` | ~0.4x; Dense faster | Strict is both cheaper in projected server work and about 7.7x smaller in total wire |
| DefraDB document by ID | 1M fixed 256-byte projections | 250 KB / 560 B | **~1.61 / ~0.33 ms `P`** | 3.30 KB / 28.0 KB | 0.0265 / 0.0042 ms `M` | ~61x slower | About 1.6 ms server and 0.3 ms client CPU is small; Dense upload makes the public partition mandatory on constrained networks |
| DefraDB secondary-index page | 1M fixed four-value pages | 250 KB / 1.10 KB | **~3.15 / ~0.33 ms `P`** | 3.90 KB / 54.8 KB | 0.0266 / 0.0039 ms `M` | ~118x slower | About 3 ms is acceptable for one private page; continuation count must be capped because high fanout multiplies work |

The snapshot decoy figures are the same-row in-process gallery measurements;
the generic 1 GiB point-read control was 0.01138 ms. Consequently, projected
ratios are planning estimates, not same-kernel claims. The important admission
test is absolute compute and wire cost: a 50x ratio can mean 1.5 ms versus 0.03
ms, while an unpartitioned 10.4-second scan is rejected even if privacy is
valuable.

The selected live protocol has a directly measured production-shaped batch:

| Live use case | Strict packed-Dense registration / response | Strict client setup / server per epoch | 100-visible registration / response | Visible client setup / server per epoch | Strict server / visible | Acceptance |
|---|---:|---:|---:|---:|---:|---|
| Mizu routing-tag alert | 16,384 B once / 2 B | **33.8 us once / 0.182 us** | 400 B once / 1,600 B | 0.240 us once / 0.206 us | **0.88x; strict 12% faster** | No server slowdown and 800x smaller recurring response |
| Shinzo contract event alert | 16,384 B once / 2 B | **33.8 us once / 0.182 us** | 400 B once / 1,600 B | 0.240 us once / 0.206 us | **0.88x; strict 12% faster** | No server slowdown; the block already supplies the natural epoch |
| DefraDB private change feed | 16,384 B once / 2 B | **33.8 us once / 0.182 us** | 400 B once / 1,600 B | 0.240 us once / 0.206 us | **0.88x; strict 12% faster** | No server slowdown; one commit/second cadence amortizes registration |

These batch-512 numbers measure resident GPU selectors. Copying selectors from
host memory every epoch adds 4.778 us/subscriber: about 24x the visible server
baseline but still under 5 us absolute. Production therefore retains
registrations on the GPU or stages long-lived pinned batches. Combining a
two-byte client answer was not isolated by the runner and is expected below
0.01 ms; transport will dominate it.

For client-network intuition, sending 80 KB takes about 64/6.4 ms at 10/100
Mbit/s; 250 KB takes 200/20 ms; the 1.082 MB nullifier query takes about
866/87 ms; and the 16,384 B live registration takes 13/1.3 ms once. These are
payload-only transfer times before RTT, OHTTP/Tor and framing. Thus note/log/
receipt queries remain phone-capable, the 1M-row generic queries prefer normal
broadband or a smaller partition, and the current nullifier path is the main
mobile optimization target.

### Production recommendation for each implemented use case

The selected defaults are summarized first. A public partition reveals only the
declared collection, generation, block range or epoch; the lookup key remains
private. “Require a bound” is an API contract, not permission to silently fall
back to visible candidates.

| Use case | Selected private protocol | Required public bound |
|---|---|---|
| Mizu wallet note recovery | Replicated Dense XOR | Committed 1/32/256-block class |
| Mizu active nullifier witness | Stable-index active-generation Dense XOR | Active-generation checkpoint |
| Mizu routing-tag alert | Packed-presence Dense | Two-second block/epoch |
| Shinzo historical contract logs | Replicated Dense XOR | 32-block default window |
| Shinzo transaction receipt | Replicated Dense XOR | Inclusion block |
| Shinzo contract event alert | Packed-presence Dense | Committed block/epoch |
| DefraDB document by ID | Replicated Dense XOR | ACP-equivalent collection/generation |
| DefraDB secondary-index page | Exact-MPHF/Fuse Dense XOR | Collection plus time/generation |
| DefraDB private change feed | Packed-presence Dense | One second or one commit |

#### 1. Mizu wallet note recovery

Use replicated Dense XOR over one committed block for a hit follow-up and a
32-block public catch-up window, with a 256-block recovery class only when its
artifact remains bounded. At the 5K TPS maximum and two-second blocks these
classes contain at most 10K, 320K and 2.56M events. Keep the fixed encrypted
projection at or below the measured 1 GiB class. A tag requiring 256
continuation pages costs about **1.58 s aggregate server work** at 6.17 ms/page;
that is a reason to shorten the window or redesign the page. Use a full/padded
routing-prefix domain when populated low-bit prefixes would permit a dictionary
attack. No higher protocol was eliminated: first-ranked cold Dense fits.

For the 32-block maximum envelope, expect about 80 KB upload, 1.61 KB download,
1.48 ms aggregate GPU server work and 0.11 ms client CPU. The same-row decoy
control used 3.10/80.4 KB up/down and 0.0284/0.0042 ms server/client. Dense is
about 52x slower on the server but adds only about 1.45 ms, while total wire is
actually almost identical. That is a good trade for strict tag privacy.

Carry each replica share through a different OHTTP relay/gateway path and use
fixed page sizes and cadence. Tor is an optional stronger-origin mode when the
measured roughly 0.88 s warm latency is acceptable.

#### 2. Mizu active nullifier witness

Use the known, stable future leaf index to address a compact projection of the
active-generation checkpoint, then retrieve the fixed authenticated witness
with replicated Dense XOR. The executed 1.05M-leaf strict path used **34.45 ms
server**, **22.29 ms client**, and 116.7 KB download. The 100-candidate control
used 0.141 ms server and 200.8 KB download, but exposed all path/index
candidates; it is not the default.

The strict request uploaded 1.082 MB versus 3.20 KB for decoys and was 244x
slower on the server. This is the one selected snapshot case where the slowdown
is material rather than merely a large ratio over a tiny baseline. It is
acceptable for occasional proof preparation; it is not acceptable as a
per-event query, and synchronized wallets should maintain/update cached
witnesses when possible.

A 32B-coordinate sparse tree must not become a 32B-row PIR table. Production
requires stable insertion indices plus authenticated sparse nodes or
checkpoints/deltas so the artifact scales with populated active state. If the
active generation itself is allowed to grow without a checkpoint bound, Dense
is eliminated by scan and rebuild cost; SinglePass is eliminated because a
wallet would need to preload and continuously update the active generation;
InsPIRe and GPU-DPF still traverse the unbounded state. Split-trust blind exact
search is then the next weaker research path. Only if that architecture is
unavailable and the candidate-set leakage is accepted should structurally
plausible, freshly sampled decoy coordinates be used.

Use OHTTP by default and Tor for high-risk proof preparation. A high-entropy
nullifier is hard to guess, but the nullifier plus the wallet IP is still an
identifier.

#### 3. Mizu routing-tag alert

Use packed-presence Dense once per public two-second block/epoch. One wallet
normally registers one routing bucket. At batch 512 it used **0.182 us aggregate
server/subscriber/epoch**, versus 32.589 us for GPU-DPF and 0.206 us for 100
visible buckets. At 5K TPS, 10K events are ORed into one bitmap before the one
answer per subscriber; one million subscribers imply about 182 ms aggregate
answer work/epoch and about 8.2 GB retained selector state/server. No higher
live protocol was eliminated: epoch batching is acceptable.

Registration is 16,384 B once and took 33.8 us/client in the batch-512 run; the
answer is 2 B/epoch. Visible candidates register 400 B and return 1,600 B/epoch.
Strict server work is 0.88x the visible baseline--about 12% faster--so this case
does not pay a PIR server slowdown at all.

Poll on a fixed cadence through padded OHTTP. Tor is optional; constant cadence
is especially important to prevent matching an alert to a later public spend.

#### 4. Shinzo historical contract logs

Use replicated Dense XOR over a public 32-block window, with one-block and
256-block classes and fixed continuation pages. At the maximum load those
classes contain 10K, 320K and 2.56M events. Admit a class only while its padded
artifact remains within the measured 1 GiB/6.17 ms-per-page budget. No higher
cold protocol was eliminated for the bounded endpoint.

At the 320K-page upper bound, expect about 80 KB upload, 1.10 KB download, 1.01
ms aggregate server work and 0.11 ms client CPU. The 100-decoy control used
3.61/54.8 KB up/down and 0.0264/0.0042 ms server/client. The roughly 38x server
ratio means only about 0.98 ms additional work. A one-block query is smaller
again, so hiding the investigated address/topic is affordable.

An unwindowed endpoint eventually exceeds 1B records. There Dense loses its
edge budget, SinglePass requires a history-sized preload and persistent state,
InsPIRe/GPU-DPF retain global traversal/capacity cost, and blind search leaks
equality/access for guessable contract/topic values. The service should require
a window. If it must offer an unwindowed degraded mode, 100 decoys are last and
their low-entropy, popularity and intersection leakage must be stated.

Use OHTTP by default, fixed hit/empty pages, and Tor for sensitive research.

#### 5. Shinzo transaction receipt

Use replicated Dense XOR over the public inclusion block. A 5K TPS/two-second
block has at most 10K receipts, far below the 1 GiB anchor; the tiny 256-row
gallery measured 2.3 us Dense server work versus 25.7 us for 100 indexed
candidates. No higher cold protocol was eliminated when the block is known.

At the maximum 10K-receipt block, the strict request is about 2.50 KB up and
368 B down, with a bandwidth projection of 0.011 ms server and below 0.01 ms
client CPU. Decoys use 3.79 KB up, 18.4 KB down and 0.0257 ms server. Dense is
projected faster and uses about 7.7x less total wire; this is the clearest cold
case where privacy is essentially free once the inclusion block is public.

If the client cannot supply the block, require it to learn a coarse inclusion
range first. A two-stage global hash-to-block directory does not create free
privacy: privately reading that billion-entry directory is itself a global PIR,
while revealing its output to the second server leaks the partition. For a
truly global endpoint, Dense, SinglePass, InsPIRe and GPU-DPF are successively
eliminated by table size, preload or aggregate work; a split-trust blind exact
directory is the next weaker option, and fresh same-class decoys are last.

Use OHTTP by default and Tor for especially sensitive transaction interest.

#### 6. Shinzo contract event alert

Use packed-presence Dense once per committed block. It has the same **0.182
us/subscriber/epoch** batch-512 evidence as the Mizu alert. A hit triggers a
bounded historical-log page, never a variable-size direct response. No higher
live protocol was eliminated. Immediate Compact DPF is used only for a real
sub-block SLA; at 5K TPS, 10K events and 10K subscribers, the measured CPU
baseline projects roughly 78 seconds of aggregate work per two-second block.

The packed registration/query geometry is the same as Mizu: 16,384 B and 33.8
us once, 2 B and 0.182 us server/subscriber/block thereafter. The visible
alternative returns 1,600 B and costs 0.206 us. Strict is 12% faster in the
measured resident batch; the reason to avoid immediate DPF is its event-times-
subscriber scaling, not the cost of privacy itself.

Use fixed-cadence padded OHTTP, independent replica operators and optional Tor.

#### 7. DefraDB document by ID

Use replicated Dense XOR for an authorization-equivalent collection/generation
artifact. The 1 GiB/8.39M-row anchor costs **6.17 ms server** and 2 MiB upload;
4 GiB/33.55M rows costs **23.07 ms** and 8 MiB upload. No higher protocol was
eliminated while the collection/generation is bounded.

For a representative 1M-row artifact of fixed 256-byte projections, expect 250
KB upload, 560 B download, about 1.61 ms server and 0.33 ms client CPU. Decoys
use 3.30/28.0 KB up/down and 0.0265/0.0042 ms server/client, making Dense about
61x slower in relative server time but only 1.58 ms slower absolutely. This is
acceptable on a desktop or normal broadband path; a phone on a constrained
uplink should use a smaller public partition rather than weaken privacy.

At a global 1B rows, require a tenant, collection, generation or public time
partition. If none is semantically safe, an independent encrypted sidecar may
use blind exact search only when the query provider cannot map tokens to
plaintext. Visible decoys remain the final high-entropy-ID fallback, not the
default. OHTTP is mandatory at the service boundary, and artifacts must be
separate for ACP-equivalent reader classes.

#### 8. DefraDB secondary-index page

Use exact-MPHF/Fuse Dense XOR inside a public collection plus time/generation
partition. Inline the fixed encrypted projection and batch continuation pages
with shared-row traversal. At 1B documents and 0.01% fanout, the executed
unpartitioned strict model cost **10.40 s server**, 38.9 ms client and 38.9 MB
download; 100 candidates cost 106.79 ms server and 31.39 ms client but **1.943
GB download**. Neither is a viable unpartitioned endpoint, so the API must
require a partition small enough for the measured 1 GiB class.

For a one-page query over 1M fixed index pages, expect 250 KB upload, 1.10 KB
download, about 3.15 ms server and 0.33 ms client CPU. Decoys use 3.90/54.8 KB
up/down and 0.0266/0.0039 ms server/client. The ratio is about 118x, but the
absolute premium is only 3.12 ms and strict returns 50x less result data. This
is acceptable for a capped page count; it ceases to be acceptable when fanout
forces hundreds of full-table continuation scans.

A two-stage design is admitted only if stage one has materially fewer distinct
keys and stage two selects a fixed padded partition without revealing the
secret tag. If stage one is as large as the documents, or the second-stage
partition ID is sent in the clear, it respectively restores the global scan or
leaks the query. If no valid bound/two-stage layout exists, reject the private
endpoint; decoys are last only when both candidate leakage and roughly 100x
result amplification are accepted.

Use OHTTP, fixed fanout/page classes and optional Tor. Encryption protects the
projection at rest but does not reduce Dense traversal.

#### 9. DefraDB private change feed

Use packed-presence Dense at a declared one-second or one-commit epoch, followed
by a bounded private snapshot page on a hit. Capacity is 8 KiB selector
state/subscriber/server, or about 8.2 GB for one million subscribers. No higher
live protocol was eliminated. Immediate Compact DPF is reserved for a genuine
sub-epoch requirement; visible subscriptions are last.

The one-time strict registration is 16,384 B/33.8 us, followed by a 2 B answer
and 0.182 us aggregate server work per epoch. The visible path is 400 B once,
1,600 B/epoch and 0.206 us server work. Strict is 12% faster on the measured
resident batch and removes 99.875% of recurring response bytes, so privacy is
not the capacity compromise in this use case.

Use fixed-cadence padded OHTTP. Durable cursors, replay and misses must retain
the same externally visible schedule; Tor remains optional.

Blind encrypted search is separate from PIR when it is used to reduce work: it
does one keyed-token lookup but leaks equality, repeats, access, volume and
update correlations. It is useful for high-entropy one-shot nullifier,
transaction-hash, or document-ID lookups only when an independent trusted
exporter holds the search/data keys and the query server never sees plaintext
mapping. Putting encrypted rows inside Dense is still useful at rest, but it
does not reduce Dense's scan.

## POC exit decision

The protocol exploration can stop here. The POC has established that DefraDB
can support private query as an isolated serving index, demonstrated the three
needed query shapes, measured their tradeoffs against visible candidates, and
carried the same requests through RFC 9458 OHTTP. The final GPU pass also found
the packed-presence epoch optimization that makes strict live alerts practical.
More protocol experiments are unlikely to reduce the main remaining risk:
building a bounded, durable DefraDB export/event adapter and validating it under
production load.

This is the exact implementation status:

| Path | Runnable default POC | Scale evidence | Deliberately left for production |
|---|---|---|---|
| Nullifier snapshot | Authenticated exact nullifier-to-2,008-byte-witness table; Dense XOR over two or more replicas; client verifies the Shieldd-shaped Poseidon path | Separate research benchmark models a 1,048,576-leaf active generation, a 32,768-nullifier block, radix predecessor retrieval and immutable deltas | Feed canonical witnesses/roots from Shieldd, wire block updates into the sidecar if live path construction is required, and add cross-repository fixture parity tests |
| Encrypted tag snapshot | Authenticated digest-to-ordinal directory and fixed padded encrypted rows; Dense XOR or visible-candidate lookup over identical rows | Separate logical benchmark executes the work/wire geometry for 1B documents, 0.01% match and exact-MPHF stripes without claiming a resident 194 GB endpoint | Choose the allowed metadata leakage, build a stable compact directory, and serve binary memory-mapped artifacts rather than JSON-loaded rows |
| Shinzo live | Immediate two-party Compact-DPF endpoints plus a correctness-checked packed-presence Dense CUDA epoch adapter; authenticated event ingestion and host adapter consume DefraDB log updates | Real local-write integration, cross-language vectors, per-event CPU baseline, and full packed Dense/GPU-DPF/visible epoch matrix | Promote packed epochs into the sidecar, then add durable cursors, persistent registrations, fixed-cadence delivery and replay/recovery policy |
| Origin hiding | Real OHTTP relay, gateway, key rotation, replay filtering, padding and direct/Tor-capable clients in the demo | Codec/HPKE benchmark at all representative payload sizes | HTTPS deployment, independent operators, anonymous admission and operational traffic-shaping policy |

The default endpoint is intentionally smaller than some benchmarked layouts.
In particular, it does **not** serve a billion-row resident table, the
nullifier endpoint does **not** consult the radix/delta engine on each request,
and the Shinzo adapter still delivers events from memory without a durable
replay cursor. The original `/v1/shinzo/event` synchronous test route remains,
while real host updates use authenticated `/v1/shinzo/events` ingestion and
per-replica `/v1/shinzo/poll` mailboxes. This proves the integration boundary,
not a production-scale live notification service.

The clean integration boundary is the result worth preserving:

```text
committed, authorized DefraDB state
  -> deterministic projection/event adapter
  -> immutable PIR artifact, packed epoch bitmap, or immediate DPF event bucket
  -> isolated PIR sidecar
```

No PIR code needs to modify CRDT merge, document storage, ACP, the query
planner, or ordinary query execution. Experimental layouts should remain under
`tools/pir-poc`; production work should begin with the adapter and artifact
format, not by moving the whole POC into DefraDB core.

## Runtime architecture

The normal binary contains five commands and three use-case paths. It remains a
DefraDB/Shieldd sidecar: an exporter supplies committed generation records, the
POC builds immutable tables, and no DefraDB query planner or storage engine is
modified.

Strict and visible-candidate modes share one table (the benchmark uses exactly
100 candidates; the endpoint admits up to the authenticated configured limit):

```text
authenticated generation
        |
        +-- exact active-nullifier witness projection
        |       +-- Dense XOR selector shares -> one fixed witness
        |       `-- 100 visible ordinals -> 100 witnesses, process one
        |
        +-- exact encrypted-tag projection
        |       +-- Dense XOR selector shares -> one fixed result row
        |       `-- 100 visible ordinals -> 100 result rows, process one
        |
        +-- Shinzo packed Dense registrations -> fixed epoch hit shares
        `-- Shinzo Compact-DPF registrations -> immediate event shares
```

The client ordinal format is a canonical, safe digest-to-ordinal directory. It
is larger than PtrHash metadata and leaks populated key digests to dictionary
attacks, but the default serving path performs no unsafe deserialization and is
stable across compiler versions. PtrHash remains a research optimization.

Every replica advertises the same operator-MACed manifest containing generation
height/root, table and directory digests, fixed result shapes and admission
limits. The client rejects divergent replicas before generating a selector.

Origin privacy is a separate layer. The POC can carry each replica share over
RFC 9458 Oblivious HTTP using a different relay/gateway path:

```text
                         opaque OHTTP request        PIR share
wallet -- HTTPS --> relay A -----------------> gateway/replica A
       `- HTTPS --> relay B -----------------> gateway/replica B
```

Each relay sees a wallet address and ciphertext but not the PIR route, selector
share or response. Each gateway sees one PIR share and a relay address but not
the wallet. Using one relay for both replicas would give that relay a stable
cross-share correlation point, so the selected topology uses independently
operated paths.

## Selected protocols and unified benchmark

Command:

```bash
cargo run -p pir-poc --release -- benchmark full
```

The table below is from a full release run of the reviewed branch. Timings are
host-specific; protocol byte counts and result schedules are deterministic.

| Use case | Private protocol | Private server time | Private client time | 100-decoy server time | 100-decoy client time | Private download | Decoy download |
|---|---|---:|---:|---:|---:|---:|---:|
| Active nullifier -> 2,008 B Merkle witness | Two-server live radix + Dense path retrieval | 34.45 ms | 22.29 ms | 0.141 ms | 0.00010 ms | 116.7 KB | 200.8 KB |
| Tag over 1B documents, 0.01% match -> 100K encrypted results | Two-server exact-MPHF striped Dense XOR | 10,398.29 ms | 38.90 ms | 106.79 ms | 31.39 ms | 38.9 MB | 1.94 GB |
| Shinzo live wallet subscription | Two-server Compact DPF | 0.00080 ms | 0.00010 ms | below timer resolution | below timer resolution[^timer] | 252 B | 204 B |

[^timer]: Below the benchmark timer's useful resolution.

Private server time is aggregate elapsed work across both replicas. These are
in-process measurements and exclude HTTP, TLS, queues and network latency.
The Shinzo row is the currently served immediate Compact-DPF path; the newer
packed-presence epoch result below is the recommended production direction and
is measured by the separate CUDA research runner.

Strict/decoy speed ratios are not security-equivalent comparisons. Decoys leak
the candidate set, result cardinality, popularity and longitudinal
intersections.

### Huge-dataset GPU PIR versus 100 visible candidates

The research benchmark below accepts the published Ethereum Reads
[`inspire-gpu`](https://github.com/keewoolee/inspire-gpu) figures and measures
our weaker baseline at the identical `2^23`, `2^25`, and `2^27` index spaces
with 120-byte records. Each visible-candidate request supplies 100 present
ordinals and returns all 100 rows. The full release run uses eleven samples and
a 388--431 MB resident, randomly distributed working set inside each exact
file-backed address space, so the reads exceed last-level cache without
pretending this host keeps an entire 16 GB table in RAM.

| Logical table | 100-candidate server p50 / p95 | Candidate throughput | InsPIRe GPU single | PIR/decoy time | InsPIRe GPU batched per query | Batched PIR/decoy time |
|---:|---:|---:|---:|---:|---:|---:|
| 1.01 GB, 8.39M rows | 11.38 / 13.53 us | 87,871 q/s | 2.6 ms | 228x | 1.73 ms | 152x |
| 4.03 GB, 33.55M rows | 12.51 / 14.00 us | 79,935 q/s | 7.9 ms | 632x | 3.88 ms | 310x |
| 16.11 GB, 134.22M rows | 12.84 / 13.87 us | 77,911 q/s | 31.1 ms | 2,423x | 8.7 ms | 678x |

The visible request is 800 bytes and its response is 12,000 bytes. The
published InsPIRe round trip is 383 KiB, 30.64x larger. This ordering is
expected: the visible path reads 12 KB regardless of `N`, while strict
double-stateless PIR evaluates the full logical database. InsPIRe nevertheless
makes strict privacy practical: its CUDA server reaches 115 q/s at 16 GB and
batch 32, while the portable client is CPU-only, needs no database hint, builds
a query in about 31 ms, and does not need a GPU.

This does **not** make visible candidates the privacy-equivalent winner. The
server learns all 100 identities, and repeated or biased candidate sets can
shrink the effective anonymity set far below 100. Strict PIR hides the selected
ordinal among all 134.22 million entries at the 16 GB scale. Use the visible
path when candidate-set privacy is acceptable; use GPU PIR when that leakage is
not acceptable.

Reproduce the local half of the comparison:

```bash
cargo run -p pir-poc --release --features research -- \
  research gpu-reference-decoy full
```

The table uses the median process result from five fresh WSL full runs; the
process-level p50 ranges were 9.67--16.04 us, 11.05--16.32 us and
11.88--15.08 us. The timed scope is warm and index-based, like `inspire-gpu`: keyword-index
lookup, page faults, HTTP/TLS, network transfer and client filtering are
excluded. The mapped address spaces are exact, but only scheduled pages are
resident, so this is not a cold-storage benchmark.

The same-host CUDA control does not change a snapshot choice. On the 1 GiB
physical table, two-server Dense used 5.79 ms aggregate/query at batch 1 and
5.64 ms at batch 128; the pinned GPU-DPF path fell from 408.24 ms to 6.46 ms
but never overtook Dense. On the largest fitting 4 GiB table and batch 128,
Dense was 23.48 ms versus GPU-DPF 28.84 ms. DPF saves upload
(4,160 B/query instead of 2--8 MiB here), not total server work. See
`COMPARISON.md` for the full correctness, energy and batch matrix.

The final five-process same-card run strengthens that conclusion without
making it universal. At 1 GiB and batches 1/8/32, InsPIRe used
32.21/20.03/18.86 ms server/query; Dense used 6.17/6.18/6.14 ms. InsPIRe has
one server, computational privacy, a 379,904-byte upload and no client database
hint; Dense has two information-theoretic replicas and a 2 MiB aggregate
upload. Thus Dense remains the server-work choice, while InsPIRe can be the
better cold-client/network or single-server choice. First-online p50 was 8.10
ms Dense, 179.01 ms InsPIRe and 446.93 ms GPU-DPF. InsPIRe's 58.27--308.05 ms
first-online range and its 9.03/5.56/3.91-second materialize/preprocess/context
phases remain explicit cold-start costs.

### Go versus Rust Dense XOR kernel

The neighboring Go DefraDB repository contains an isolated port under
`tools/pir-poc`; it changes no database or API code. The Rust comparison entry
point is:

```bash
cargo run -p pir-poc --release --example cross-language-dense -- full
```

The matching Go command is:

```bash
go build -trimpath -o pir-poc-bench ./tools/pir-poc
./pir-poc-bench -profile full
```

Both executables generate byte-identical deterministic tables, confirmed by an
FNV-1a checksum, then use fresh cryptographic randomness for every Dense XOR
selector. They use the same targets, warmups, sample counts and validation.
The public and decoy paths carry 8-byte ordinals and deliberately exclude tag
hashing, directory construction and database lookup: this section compares
retrieval kernels, not the complete selected-use-case endpoints above.
Client phases run for at least 10 ms and server paths for at least 50 ms before
being divided into per-operation time. `server_total` is aggregate work: the
sum of sequential evaluation across replicas, not concurrent wall time.

These are medians across three alternating full run pairs on 2026-08-25. Each
full run itself reports the median of 11 samples for the single-query workloads
and seven for batch-16. The host was Windows 11, Ryzen 7 3700X, 16 GB RAM, Rust
1.94.0/LLVM 21.1.8 and Go 1.25.9 with `GOMAXPROCS=1`. Both kernels are
single-threaded. Compilation, table construction, HTTP and storage are
excluded.

| Workload | Path | Rust server work | Go server work | Go delta |
|---|---|---:|---:|---:|
| 1,048,576 x 96 B, batch 1 | Public/direct | 0.083 us | 0.072 us | -13.3% |
| | 100 visible candidates | 0.520 us | 1.766 us | +239.6% |
| | Dense XOR, 2 replicas | 15.04 ms | 19.49 ms | +29.6% |
| | Dense XOR, 3 replicas | 22.21 ms | 29.50 ms | +32.9% |
| 65,536 x 2,008 B, batch 1 | Public/direct | 0.104 us | 0.374 us | +259.6% |
| | 100 visible candidates | 4.893 us | 29.777 us | +508.6% |
| | Dense XOR, 2 replicas | 12.55 ms | 15.90 ms | +26.7% |
| | Dense XOR, 3 replicas | 18.40 ms | 23.38 ms | +27.1% |
| 262,144 x 96 B, batch 16 | Public/direct | 0.885 us | 0.697 us | -21.2% |
| | 100 visible candidates | 11.224 us | 31.647 us | +182.0% |
| | Dense XOR, 2 replicas | 58.74 ms | 78.60 ms | +33.8% |
| | Dense XOR, 3 replicas | 89.29 ms | 117.38 ms | +31.5% |

The direct and decoy percentages magnify sub-32-us allocator/copy differences;
their absolute work remains negligible beside Dense XOR and they do not offer
equivalent privacy. Dense XOR is the decision-relevant result: Rust used about
21-25% less server time, or equivalently Go was 27-34% slower. A third replica
adds about 50% total server work and wire bytes in both languages, as expected.

| Workload | Replicas | Rust client query | Go client query | Go delta | Rust finish | Go finish |
|---|---:|---:|---:|---:|---:|---:|
| 1,048,576 x 96 B, batch 1 | 2 | 0.0584 ms | 0.0754 ms | +29.2% | 0.000094 ms | 0.000104 ms |
| | 3 | 0.1066 ms | 0.1048 ms | -1.7% | 0.000096 ms | 0.000116 ms |
| 65,536 x 2,008 B, batch 1 | 2 | 0.00232 ms | 0.00490 ms | +111.1% | 0.000217 ms | 0.000487 ms |
| | 3 | 0.00443 ms | 0.00825 ms | +86.5% | 0.000264 ms | 0.000498 ms |
| 262,144 x 96 B, batch 16 | 2 | 0.1411 ms | 0.2807 ms | +99.0% | 0.00147 ms | 0.00165 ms |
| | 3 | 0.5005 ms | 0.5087 ms | +1.6% | 0.00151 ms | 0.00202 ms |

Client percentages are less stable because the operations are tiny, but every
measured client phase is below 0.51 ms. Language choice therefore does not
change the protocol decision: total server evaluation remains the bottleneck.
If minimizing that work is paramount, retain the Rust serving kernel behind
the existing sidecar boundary. The native Go port is viable when operational
integration is worth a measured 27-34% Dense server tax; a production Go path
should next evaluate an inlined SIMD/assembly XOR kernel and shared-row batch
traversal before accepting that tax.

Wire geometry is identical: the locator workload uploads/downloads 256 KiB/192
B with two replicas and 384 KiB/288 B with three; witness uses 16 KiB/4,016 B
and 24 KiB/6,024 B; batch-16 uses 1 MiB/3,072 B and 1.5 MiB/4,608 B.

### OHTTP transport benchmark

The unified benchmark also exercises the real RFC 9458 HPKE and Binary HTTP
implementation at representative per-replica payload sizes. These measurements
isolate origin-hiding cryptography and framing; they exclude PIR evaluation,
TCP, TLS, relay latency and queues. Release-mode runs on the development
machine produced representative values:

| Per-replica payload | Padding | Request wire | Response wire | Client codec + crypto p50 | Gateway codec + crypto p50 |
|---|---|---:|---:|---:|---:|
| Compact-DPF representative: 320 B request, 126 B response | None | 489 B | 195 B | 0.133 ms | 0.092 ms |
| Compact-DPF representative | Power of two | 567 B | 288 B | 0.120 ms | 0.089 ms |
| Compact-DPF representative | Fixed 1 KiB/1 KiB | 1,079 B | 1,056 B | 0.127 ms | 0.101 ms |
| Active-nullifier share: 541,241 B request, 58,344 B response | None | 541,412 B | 58,415 B | 1.031 ms | 0.628 ms |
| Active-nullifier share | Power of two | 1,048,631 B | 65,568 B | 1.788 ms | 1.164 ms |
| 1B-tag share: 1,250 B request, 19,428,008 B response | None | 1,419 B | 19,428,079 B | 49.79 ms | 77.99 ms |
| 1B-tag share | Power of two | 2,103 B | 33,554,464 B | 76.43 ms | 117.62 ms |

Unpadded OHTTP adds approximately 55 request bytes and 32 response bytes beyond
Binary HTTP. For active-nullifier and tag retrieval, PIR traversal and large
answer authentication dominate the small HPKE setup. For Compact DPF, OHTTP
dwarfs its microsecond evaluator but remains around a fraction of a millisecond
per party before network latency. Consult the current JSON output for timing;
the byte classes above are the stable deployment inputs.

Power-of-two padding is not the default recommendation for large result rows:
it inflated the 19.4 MB answer to 33.6 MB and increased gateway crypto time by
about 51%. Production should use route-specific fixed public result classes
close to the authenticated manifest sizes. A fixed class makes valid success
and application-error ciphertexts the same length; no padding and power-of-two
classes leak their respective size class.

## Protocol overview

### 1. Active-nullifier private retrieval

The runnable endpoint uses the simplest complete result: a canonical nullifier
maps directly to one supplied fixed 2,008-byte Shieldd-shaped witness. Dense
XOR privately retrieves that padded row, and the client verifies it against the
authenticated generation root. This avoids a second private document fetch.

The separate active-generation research benchmark represents Shieldd state as
one immutable base plus small authenticated delta levels. Its proposed private
lookup has two fixed-schedule stages:

1. privately retrieve the linked predecessor leaf from a radix layout;
2. privately retrieve the sibling rows at every level of the quaternary Merkle
   path.

For every selector, the client generates random XOR shares whose XOR is the
real selector. Each replica sees only its random share, scans its local copy and
returns an answer share. XORing every answer reconstructs the fixed 2,008-byte
witness. The generation height and root are public, but the nullifier has
information-theoretic target privacy while at least one required replica does
not collude with the others.

The base/delta engine is built, authenticated and benchmarked, but the default
HTTP nullifier endpoint does not yet derive witnesses from it. Production can
keep the direct witness projection when Shieldd exports witnesses cheaply, or
wire the radix/path engine when storing every populated witness is too costly.

### 2. Encrypted-tag Dense XOR and the exact-MPHF scale layout

The runnable endpoint uses the safe canonical digest-to-ordinal directory. It
is deliberately straightforward and suitable for demos and moderate exported
collections; it is not the billion-document directory implementation.

In the scale benchmark, a public exact minimal-perfect hash function maps every populated tag to a
compact ordinal, avoiding an overprovisioned hash table. The tag's results are
stored in fixed continuation stripes. In the one-billion-document benchmark,
the target has 100,000 matches, each containing five encrypted fields in one
188-byte value, spread over 391 stripes of 256 values.

The client generates XOR shares of one Dense selector and reuses that selector
across the fixed stripe schedule. Each replica traverses the encrypted
projection and returns one answer share per stripe; the client XORs the shares
and authenticates/decrypts the 100,000 target ciphertexts. This gives
information-theoretic tag privacy under the same non-collusion assumption. The
result-size class and populated-key metadata are public in this POC.

The strict query scans the projection, but downloads only target results. The
100-decoy alternative performs cheap ordinary lookups but returns ten million
ciphertexts. The client authenticates/decrypts its 100,000 target values and
discards the other 9.9 million without AEAD work.

### 3. Packed-presence Dense live subscription

The preferred live flow uses a declared public cadence: one Ethereum block,
one second, or another fixed epoch. Events set bits in a fixed 65,536-bucket
presence bitmap. At registration, the client sends each replica one random
8 KiB Dense selector share; XORing all shares gives a one-hot vector for the
private bucket. At epoch close, each server computes one packed parity answer,
and the client XORs the answer bytes. A hit triggers the normal private padded
snapshot fetch for that epoch.

This is information-theoretically private under the same non-collusion model as
snapshot Dense, exact when a bucket occurs more than once, and works with two,
three, or more replicas. Selectors are reusable across epochs: reuse links one
subscription at a server but does not reveal its uniformly hidden bucket. The
cost is 8 KiB of retained selector state per subscriber per server and epoch
latency rather than immediate notification.

On the RTX 2070 SUPER, a batch of 512 ready subscriptions cost 0.182 us
aggregate strict kernel time/subscriber/epoch, returned 2 B across two replicas,
and was slightly below the matched one-server 100-visible-bucket control's
0.206 us. The visible path is still weaker privacy and returns 1,600 B. One
million strict subscriptions require about 8.2 GB selector storage/server and
about 0.18 seconds aggregate kernel work/epoch if selector batches stay on the
GPU. Copying them from host memory every epoch adds about 4.78 us/subscriber at
batch 512 and must not be omitted from capacity planning.

Compact DPF remains the fallback when the application truly requires an answer
for every individual event. It stores only 2,080 B/server/subscription, but the
current implementation is computational, exactly two-party, and evaluates
every subscription for every event. Its default HTTP endpoints and the
research-only `research defra-events` command still prove that immediate flow;
the packed epoch protocol is currently a correctness-checked CUDA research
adapter, not yet the default sidecar API.

## Adding a third server

| Private process | Can add a third server? | Effect |
|---|---|---|
| Active nullifier radix/path Dense XOR | Yes, without changing the table or cryptographic construction | Generate three XOR shares and combine three answers. Aggregate upload, response traffic and server work rise by about 50% relative to two servers. |
| Exact-MPHF striped Dense XOR | Yes, without changing MPHF, stripes or rows | The same `n`-server sharing works for any `n >= 2`; a three-server deployment again costs approximately 50% more aggregate work than two servers. |
| Packed-presence Dense subscription | Yes, without changing the public bitmap | Store one 8 KiB share/subscriber on each server and XOR all answer bits. Three servers add about 50% aggregate kernel/storage/registration cost relative to two. |
| Compact DPF subscription | No, not as a drop-in change | The current DPF construction and wire format produce exactly two keys and require exactly two answers. Three-party support needs a multi-party FSS/DPF construction or a different live protocol. |

The Dense extension is flexible about server count, but it is `n`-of-`n`: all
configured answers are needed to reconstruct the result. A third server
strengthens the non-collusion assumption only if the deployment actually
provides another independent trust domain; it does not automatically improve
availability. Threshold reconstruction such as two-of-three would be a
separate construction, not merely another XOR share. The benchmark numbers in
this document use two replicas.

## 1. Active Shieldd nullifier generation

Shieldd uses a depth-20 quaternary indexed nullifier tree with sequential leaf
positions. A non-membership witness contains a linked predecessor leaf and 60
sibling hashes, encoded as one fixed 2,008-byte result.

The full benchmark starts with 1,048,576 ordinary nullifiers and applies one
maximum 32,768-nullifier block. The rejected flat sidecar rewrites every one of
4,096 radix rows plus changed node rows.

| Active block update | Flat padded layout | Implemented immutable delta |
|---|---:|---:|
| Build/update time | 156.74 ms | 89.68 ms p50 |
| Payload written/replica | 235,938,720 B | 11,859,028 B |
| Amplification over raw 32-byte inserts | 225.01x | 11.31x |
| Relative result | baseline | 19.90x fewer bytes; 1.75x faster construction |

The implemented layout has:

- one immutable base;
- up to eight geometric immutable delta levels;
- newest-wins linked-leaf mutations;
- copy-on-write node-coordinate overrides;
- a fixed nine-level predecessor schedule;
- a fixed `9 * 60` node-probe schedule, including empty levels;
- off-path construction followed by one generation-pinned `Arc` publication;
- height/root/body-digest binding and operator MAC;
- image corruption and stale-publication rejection.

Overflow merges geometrically and eventually creates a new immutable base.
Readers that pinned the old generation continue to see it until completion.

The endpoint serves an exact populated nullifier-to-witness projection for the
published current generation. The delta engine and benchmark cover live block
mutation/publication; direct authenticated block ingestion is intentionally not
an unauthenticated public HTTP endpoint.

Strict query versus decoys:

- Strict Dense baseline: 34.45 ms aggregate server, 22.29 ms client, 1,082,482 B
  upload and 116,688 B download.
- Decoys: 0.141 ms server and 200,800 B download.
- Strict used 244.12x more server elapsed and saved 1.72x download.
- The decoy client parses only the target witness and ignores the other 99.

The remaining nullifier query optimization is a tree-specific path PIR. The
ordinary Dense baseline sends many level-specific selectors; this is now a
clearly isolated optimization rather than another storage redesign.

## 2. Encrypted tag projection at one billion documents

The benchmark has 10,000 equal-cardinality tags and 100,000 matches for the
target (`0.01%`). Each result carries five encrypted 32-byte fields as a
188-byte AEAD-shaped value. One selector is reused over 391 fixed continuation
stripe planes.

The logical immutable projection is 194.28 GB per replica. The benchmark
executes every logical scan over one resident 496.88 MB representative plane;
it preserves XOR count and wire geometry without claiming deployed 194 GB
memory behavior.

- Strict Dense: 10.40 seconds server, 38.86 MB response.
- 100 decoys: 106.79 ms server, 1.943 GB response.
- Strict used 97.37x more server elapsed.
- Decoys downloaded 50x more data.
- Both decrypt only the target's 100,000 ciphertexts. The decoy client ignores
  9.9 million non-target ciphertexts without AEAD.

No Dense micro-optimization removes the full encrypted-projection traversal.
For this workload the privacy-first policy is explicit:

- require a public immutable time/collection window small enough for strict
  Dense, or a valid private two-stage partition;
- reject the unpartitioned endpoint when that cannot be done;
- use split-trust encrypted exact search only when its equality/access leakage
  is accepted;
- use 100 decoys only as the final degraded mode when candidate visibility and
  the 1.943 GB response are both explicitly accepted.

Ciphertext projection data is effectively incompressible. Three replicas add
approximately 50% aggregate Dense work and are an availability/trust choice,
not a speed optimization.

## 3. Shinzo wallet-event subscription

The selected production direction is a fixed public block/epoch plus private
packed-presence Dense over a 65,536-bucket domain. Shinzo logs already have
block boundaries, so waiting for the block's committed bitmap is normally a
natural rather than artificial delay. A hit tells the wallet to issue a padded
private snapshot query for the matching log page; it does not expose or return
the log by itself.

At 512 ready subscriptions on the local RTX 2070 SUPER:

- packed-presence Dense: 0.000182 ms aggregate server kernel/subscriber/epoch,
  about 0.000093 ms parallel latency, and 2 B response across two replicas;
- GPU DPF over an overprovisioned 16-byte histogram: 0.032589 ms aggregate;
- 100 visible buckets: 0.000206 ms on one CPU server and 1,600 B response;
- one-time registration: 16,384 B Dense, 4,160 B GPU DPF, or 400 B visible.

The strict result being slightly faster in elapsed time than visible candidates
is hardware- and batching-specific, not a security-equivalent universal
speedup. It is enough to remove server computation as the reason to choose
decoys for epoch alerts. Dense still needs 8 KiB retained selector state on
each server, fixed-cadence responses, independent operators, and GPU residency
or explicitly charged PCIe transfers.

The already implemented immediate Compact-DPF endpoint remains useful only if
Shinzo needs sub-block notification. Its verified baseline is 0.00080 ms
aggregate server work/subscription/event with a 640 B two-party registration,
but total work and fixed delivery grow with every event and subscription. At
the 5,000 TPS maximum, a two-second epoch contains 10,000 events: for 10,000
subscribers, the measured models are about 1.82 ms aggregate packed GPU kernel
per epoch versus about 78 seconds aggregate immediate CPU DPF work. At lower
TPS the exact saving falls linearly, while packed presence remains one fixed
retrieval per epoch.

## HTTP surface

The demo and `serve` command expose:

```text
GET  /v1/manifest
POST /v1/nullifier/private
POST /v1/nullifier/decoy
POST /v1/tag/private
POST /v1/tag/decoy
POST /v1/shinzo/register
POST /v1/shinzo/event
POST /v1/shinzo/events
POST /v1/shinzo/poll
```

Private endpoints accept only one replica's opaque selector shares. Decoy
endpoints accept visible candidates. Shinzo registration accepts one Compact-DPF
key share for that party.
`/v1/shinzo/events` is a direct, bearer-authenticated host-to-sidecar route and
never returns result shares. `/v1/shinzo/poll` is admitted through direct HTTP
or OHTTP and returns that replica's bounded mailbox entries.

## OHTTP origin-hiding POC

The implementation uses X25519/HKDF-SHA256/AES-128-GCM HPKE and Known-Length
Binary HTTP. It includes:

- operator-MACed public gateway key documents bound to the PIR generation;
- current/previous receive-key rotation;
- a bounded replay cache checked before state-changing dispatch;
- a fixed-destination relay that strips client headers and records only
  aggregate request/byte counters;
- fixed gateway authority, method/path allow-list and header allow-list;
- bounded zero padding with none, power-of-two and fixed strategies;
- the same selected-service dispatcher as direct HTTP;
- two-path Dense XOR and Compact-DPF clients.
- a minimal anonymous-HTTP transport seam with direct and Tor-compatible
  `socks5h` backends.

Tests cover all three private protocols, ciphertext tampering, replay, key
rotation/expiry, malformed and oversized padding, authenticated metadata, and
equal fixed-size success/error responses. `pir-poc demo` exercises the complete
client -> relay -> gateway -> selected-service flow. It also reports cold setup
plus first-query/p50/p95 verified latency over 11 queries for one visible direct
lookup, direct Dense XOR and Dense XOR over OHTTP. Three environment variables
add a real Tor + OHTTP row: `PIR_POC_TOR_SOCKS_URL`,
`PIR_POC_TOR_RELAY_URLS`, and `PIR_POC_OHTTP_RELAY_BINDS`. When Tor is not
configured, the row is explicitly marked not run.

### Real Tor/onion benchmark

This run used the signed Tor Expert Bundle 15.0.20 (Tor 0.4.9.11), Windows 11,
an AMD Ryzen 7 3700X and Rust 1.94.0 on 2026-08-25. Two v3 onion services mapped
to the POC's two local OHTTP relays. The client used `socks5h` and separate
SOCKS-auth isolation tokens for the replica paths. Every reported query
reconstructed and verified the requested nullifier witness.

| Path | Setup (ms) | First query (ms) | p50 (ms) | p95 (ms) | Encrypted upload/query | Encrypted download/query |
|---|---:|---:|---:|---:|---:|---:|
| Visible direct HTTP | 5.35 | 6.49 | 4.96 | 6.49 | not instrumented | not instrumented |
| Two-server PIR, direct HTTP | 8.78 | 5.21 | 5.09 | 5.21 | not instrumented | not instrumented |
| Two-server PIR, OHTTP loopback | 8.26 | 5.73 | 5.90 | 15.14 | 8,302 B | 131,136 B |
| Two-server PIR, Tor + OHTTP, cold circuits | 5,330.55 | 1,160.26 | 1,258.54 | 1,374.52 | 8,302 B | 131,136 B |
| Two-server PIR, Tor + OHTTP, warm[^tor-warm] | 2,026.60 | 863.92 | 882.98 | 1,186.26 | 8,302 B | 131,136 B |

[^tor-warm]: Median of three run-level results; every run contains 11 verified queries. Individual warm p50 values were 867.56, 882.98 and 896.43 ms.

Fresh Tor bootstrap took about 8.1 seconds; restart with cached directory state
took about 3.9 seconds. During three warm runs Tor used 125.64 MiB working set,
96.53 MiB private memory, and 2.406 CPU-seconds over 38.884 wall-seconds (about
0.062 average CPU core). These are desktop measurements, not phone/battery
results.

The OHTTP byte counts are exact relay payload counters summed over both
replicas; they exclude Tor cell framing, circuit handshakes and TCP/TLS
overhead. Fixed envelopes make the OHTTP and Tor rows equal at the application
layer. The two onion services and both POC servers ran on this machine, so this
is a real Tor rendezvous-path measurement but not evidence of administrative
non-collusion or geographically remote provider latency.

The conclusion is narrow: Tor works without changing PIR/OHTTP and does not add
PIR server evaluation, but warm verified latency was about 150x loopback OHTTP
in this run. Keep OHTTP as the low-latency default and Tor/onion as an explicit
strong-origin mode until native mobile Arti is measured on a phone.

The OHTTP layer hides origin from a non-colluding relay/gateway pair; it does
not change PIR server work or its non-collusion requirement. It also does not
defeat a colluding relay and gateway, a global timing observer, a compromised
client, or result-size correlation outside the selected padding class. The POC
uses whole-message buffers. This is acceptable for the 36 KB nullifier response
and possible but memory-relevant for a 19.4 MB tag share; production should
measure peak mobile memory before enabling that result class.

## Production boundary

The POC now enforces:

- immutable versioned generation directories;
- fsync before publication and no overwrite of an existing generation;
- operator-MACed manifests obtained through an out-of-band 32-byte key;
- safe, size-bounded ordinal metadata;
- query, response, key, batch, table, metadata, transient-memory, in-flight and
  subscription limits;
- fixed result schedules and generation IDs on every request/response;
- duplicate Compact-DPF subscription rejection;
- client agreement across replicas;
- absent-key row fingerprint rejection.
- canonical Shieldd indexed-nullifier witness verification using the 20-level,
  4-ary Poseidon construction;
- AES-256-GCM projection authentication bound to generation, tag and slot;
- a direct/Tor origin-transport boundary that leaves PIR and OHTTP framing
  unchanged.
- fixed local relay binds plus independently addressed Tor relay URLs for real
  onion-service testing;
- per-replica SOCKS-auth circuit isolation and first/p50/p95/byte reporting.

Before production, replace the symmetric operator MAC with the deployment's
signature/key-distribution system, obtain the Shieldd root through consensus or
a light client rather than the serving replicas, mmap large tables, move the
canonical witness verifier into a shared Shieldd SDK crate, protect projection
keys in the wallet key hierarchy, and add operational metrics and authenticated
block-ingestion authorization.
For OHTTP specifically, terminate HTTPS on both hops, deploy relays and
gateways in independent trust/administrative domains, authenticate key
documents with the production signature chain, add abuse controls that do not
introduce stable wallet identifiers, define a rotation overlap in time rather
than only key count, and evaluate batching/jitter if spend-timing correlation
is in scope.

## What could change this decision in the future

This ranking is a benchmark-backed decision for August 2026, not a permanent
claim about PIR. Ethereum's [Reads private-state workstream](https://reads.ethereum.foundation/workstreams/pir/)
has converged on the same high-level conclusion as this POC: use different
engines for hot mutable state, proof-carrying state, immutable logs and archival
data rather than forcing one PIR construction over every query. The following
advances would justify rerunning the ranking.

| Potential advance | Evidence/status now | Measurable trigger for changing this POC | Use cases most affected |
|---|---|---|---|
| Production GPU InsPIRe | Ethereum Reads reports 3.2 ms/1 GB, under 10 ms/4 GB and 36 ms/16 GB on one RTX 5090, roughly 300--400 KB round trip, under 2x memory expansion and no per-client server state ([May/June update](https://reads.ethereum.foundation/feed/update-may-june-2026/)) | On identical hardware/corpus, beat Dense's total server work or offer a deployment-winning one-server trust/wire result without unacceptable client CPU | Global receipt/document lookup, hot nullifier state and cold clients where Dense upload is too large |
| VIA, OnionPIRv2 or a later double-stateless scheme | VIA has a Rust implementation under parameter review; OnionPIRv2 and VIA are being compared with InsPIRe on GPU ([Ethereum scheme list](https://reads.ethereum.foundation/workstreams/pir/)) | Audited >=128-bit parameters plus lower measured server joules/query and acceptable wire on the same 1/4/16 GiB matrix | Could replace InsPIRe as the one-server fallback; does not displace Dense merely by reducing wall time through more parallel hardware |
| Skirrt-style batched proof PIR | Ethereum Reads says its in-progress design retrieves a complete Merkle proof in one double-stateless batch with >10x lower communication than prior batching ([May/June update](https://reads.ethereum.foundation/feed/update-may-june-2026/)) | Beat the active-nullifier baseline of 34.45 ms server, 22.29 ms client, 1.082 MB upload and 116.7 KB download while supporting live checkpoint updates | Mizu active nullifier witness first; any authenticated document/path query second |
| Private sharding or private two-stage routing | Ethereum is designing sharded PIR for 1--10 GB hot slices, 100--300 GB proof state, hundreds-of-GB immutable logs and 2--30 TB archives. Raven already re-encodes changed shards, but explicitly sends the shard ID in the clear ([Raven](https://github.com/hisoka-io/raven)) | Hide the selected shard, or prove that a coarse public shard leaks nothing sensitive, without restoring a global first-stage scan | Unwindowed receipts/logs, billion-row document IDs and secondary indexes; this is the advance most likely to remove mandatory public windows |
| Immutable preprocessing schemes | Harmony/RMS24 are being developed for immutable or slowly changing slices where hint construction is amortized ([Ethereum scheme list](https://reads.ethereum.foundation/workstreams/pir/)) | End-to-end lifetime work, client state and generation refresh beat SinglePass/Dense at the real query count | Historical logs, receipts and old Mizu generations; not the active generation |
| PIR-friendly authenticated state layouts | Ethereum's Verifiable UBT work explores a flat binary trie and proof binding; the current analysis reports about 9x PIR read overhead for the binary tree versus 48x for the MPT ([COSIC presentation](https://reads.ethereum.foundation/presentations/cosic-applied-crypto/index.html)) | A production, incrementally updated authenticated layout with small proofs and independently verifiable root equivalence | Nullifier paths, DefraDB authenticated projections and two-stage indexes; it improves representation, not the underlying linear PIR law |
| Universal PIR and access-layer interfaces | Ethereum is building middleware that keeps wallet RPC stable while shards/schemes change, plus a network-agnostic origin layer ([projects](https://reads.ethereum.foundation/projects/)) | A stable audited interface and implementations that can be selected per slice without client forks | Validates this POC's sidecar/adapter boundary and makes adding a new backend cheaper; it does not itself change benchmarks |
| Embedded Tor/Arti maturity | Ethereum Reads has a functional Arti-to-WASM wallet prototype and TorJS, but still lists audit, WASM isolation, fingerprinting and bootstrap caveats ([engineering report](https://reads.ethereum.foundation/feed/embedding-arti-in-the-browser/)) | External audit plus phone battery/memory/latency results that beat the present roughly 0.88 s warm desktop path | Could make Tor rather than OHTTP the normal origin layer; it never replaces PIR because it hides who asked, not what was asked |

Two thresholds prevent ordinary incremental progress from being mistaken for a
breakthrough:

- The unpartitioned 1B-document/0.01%-fanout tag query needs about a **100x
  server-work reduction** (10.40 s toward the 106.79 ms decoy control), or
  genuinely private partition selection. A 2--5x faster GPU does not change the
  product decision.
- Immediate live PIR must improve from 32.589 us to around **0.182
  us/subscriber**--roughly 180x--or remove the `events * subscribers` scaling
  before it displaces packed epochs. Faster event-by-event DPF that remains
  above that threshold does not change the three live recommendations.

Multi-GPU execution can make a huge query return sooner, but the objective here
is aggregate server work and energy. Parallelizing the same scan changes
latency, not the ranking, unless a future construction also reduces bytes read,
joules consumed, or trust/client costs. Conversely, a mature one-server scheme
may be worth some extra work because removing the non-collusion assumption is a
privacy/deployment improvement rather than a speed claim.
