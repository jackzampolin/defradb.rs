# Selected PIR use cases

This is the authoritative POC decision record. Historical protocol exploration
is indexed in [the research archive](research/README.md), including the full
[comparison](research/COMPARISON.md) and [exploration log](research/EXPLORATION.md).

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
| Mizu routing-tag retrieval stage | At most 320K populated routing pages in 32 blocks | 80.0 KB / 1.61 KB | **~1.48 / ~0.11 ms `P`** | 3.10 KB / 80.4 KB | 0.0284 / 0.0042 ms `M` | ~52x slower | About 1.45 ms extra server work; total wire is nearly equal (81.6 versus 83.5 KB) |
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
| Mizu routing-tag alert and retrieval | Packed-presence Dense alert + replicated Dense retrieval | Two-second block/epoch plus committed 1/32/256-block retrieval class |
| Mizu active nullifier witness | Stable-index active-generation Dense XOR | Active-generation checkpoint |
| Shinzo historical contract logs | Replicated Dense XOR | 32-block default window |
| Shinzo transaction receipt | Replicated Dense XOR | Inclusion block |
| Shinzo contract event alert | Packed-presence Dense | Committed block/epoch |
| DefraDB document by ID | Replicated Dense XOR | ACP-equivalent collection/generation |
| DefraDB secondary-index page | Exact-MPHF/Fuse Dense XOR | Collection plus time/generation |
| DefraDB private change feed | Packed-presence Dense | One second or one commit |

#### 1. Mizu routing-tag alert and retrieval

This is one product flow with two protocol stages, not two independent use
cases.

##### Retrieval stage

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

An online wallet first receives the alert-stage presence bit below, then
retrieves this one-block page on a hit. An offline wallet skips registration and
uses the same endpoint for a 32-block catch-up query.

##### Alert stage

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

Register the routing-tag selector once, evaluate it once per committed block,
and issue a one-block Dense note-page query only after a hit. At the maximum
block size that hit fetch is approximately 2.50 KB up, 1.61 KB down, 0.05 ms
aggregate server and below 0.01 ms client CPU. A periodic 32-block snapshot
costs 1.48 ms once, or about 46.3 us average server work/block; resident packed
presence costs only 0.182 us/block before hits, about **254x less**. It remains
cheaper until a wallet matches nearly every block.

Do not evaluate Compact DPF for every event: that restores `events *
subscribers` work. A foreground/warm wallet should use packed presence plus the
one-block hit fetch. A sleeping or intermittently connected wallet should issue
one 32-block catch-up query when it wakes; keeping a two-second network poll
alive merely to save PIR compute is not worth mobile radio/battery cost.

Fetching only after a hit exposes hit-correlated traffic to a global observer
even though the PIR target and OHTTP origin remain hidden from non-colluding
roles. The low-latency mode accepts that timing signal. A maximum-privacy mode
delays the hit fetch into a fixed scheduled window or sends indistinguishable
dummy fetches; that traffic policy, not the PIR primitive, decides timing
privacy.

Poll on a fixed cadence through padded OHTTP. Tor is optional; constant cadence
is especially important to prevent matching an alert to a later public spend.

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

#### 3. Shinzo historical contract logs

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

#### 4. Shinzo transaction receipt

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

#### 5. Shinzo contract event alert

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

#### 6. DefraDB document by ID

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

#### 7. DefraDB secondary-index page

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

#### 8. DefraDB private change feed

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

## Supporting documents

The decision record ends here. Implementation and evidence details are kept
once, in the document that owns them:

- [README.md](README.md) is the operating guide and HTTP/CLI surface;
- [PRODUCTION.md](PRODUCTION.md) defines the minimal DefraDB adapter,
  artifact, authorization, and deployment boundary;
- [PRIVACY.md](PRIVACY.md) defines OHTTP, Tor, timing, admission, and
  write-path privacy;
- [USE_CASE_GALLERY.md](USE_CASE_GALLERY.md) is the runnable 256-row fixture
  catalog;
- [research/README.md](research/README.md) indexes protocol comparisons,
  large-scale benchmarks, GPU/CPU artifacts, and reproduction instructions.

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
