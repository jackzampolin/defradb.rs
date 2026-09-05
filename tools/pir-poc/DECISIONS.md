# PIR decisions

**Updated 2026-09-05. Objective: minimize aggregate server work while keeping
client work reasonable. Database/index disclosure is acceptable; query privacy
is required.** These are research selections, not changes to production defaults.

For bounded cold payload lookups, prefer **a public key directory plus Dense
PIR**, with the group size chosen for payload width and match count. Every client
downloads the same directory and navigates locally. The selected key, prefix and
block remain private under the two-replica noncollusion assumption. Boundary
keys are public; payload retrieval is private.

First apply a public collection, block or time filter **only if the user accepts
that scope and it contains the complete answer**. Full recovery must cover all
required history. Splitting a full scan across windows or machines does not
reduce aggregate work by itself. Directory lookup does not reveal the selected
directory entry or require narrowing the requested answer.

## Cold payload searches

Five repetitions per configuration, fresh clients and persistent native replicas.
Numbers below are **summed serving-phase CPU milliseconds**, including fresh
metadata delivery. Generation and whole-process costs are separate in the
[measurement report](research/INDEXED_USE_CASE_MEASUREMENTS.md). These are complete
lookups over the stated synthetic fixtures, including absent values and all
matches, rather than the earlier one-page projections.

| Use case | Tested source rows / payload bytes / matches per key | Directory group | Indexed Dense ms | XOR dictionary + Dense ms | Decision |
|---|---|---:|---:|---:|---|
| Mizu routing-tag retrieval | 65,536 / 804 / 4 | 4 | **5.14** | Index-size gate | Prefer indexed Dense at this scope; at 4,096 rows it measured 0.91 vs 1.07 ms |
| Shinzo historical logs | 65,536 / 548 / 4 | 4 | **3.94** | 4.98 | Prefer indexed Dense for the tested complete equality-filter results |
| Shinzo receipt, known block | 10,000 / 184 / 1 | 4 | **0.72** | 0.92 | Prefer indexed Dense here; the 1,024-row fixture is effectively tied with XOR (0.37 vs 0.36 ms) |
| DefraDB document by ID | 65,536 / 256 / 1 | 16 | **2.29** | 4.87 | Prefer indexed Dense for the tested fixed projection |
| DefraDB secondary index | 65,536 / 120 / 16 | 1 | **1.51** | 1.72 | Prefer the complete-result directory layout; the advantage over XOR is modest |
| Global receipt lookup | 32,768 / 184 / 1 | 16 | **1.25** | 2.31 | Validated on this entire fixture without revealing an inclusion partition |
| Global document lookup | 32,768 / 256 / 1 | 16 | **1.51** | 2.60 | Same, over the whole tested corpus |
| Global secondary index | 32,768 / 120 / 64 | 1 | **1.39** | 1.55 | Useful at this scope; do not extrapolate to a billion documents |
| Skewed secondary index | 16,384 rows; one value has 2,048 matches | — | Unqualified | Index-size gate | No qualified choice; directory padding exceeded index or response caps |

Payload widths are fixed benchmark projections plus record framing. Keys are
synthetic 64-bit hashes; these runs do not validate full production key widths,
arbitrary compound predicates, live application exports or million-row serving
capacity. Groups mean **distinct keys per answer block**, with all duplicates
kept together. A hot value can inflate every padded block.

The earlier 262,144-row, 32-byte tag result remains in its own scope:
directory/Dense 2.04 ms, XOR/Dense 5.09 ms, hashed-page Dense 15.47 ms. Its roughly
64 KiB directory is not a universal setting. Wider results need different groups.
See [the earlier campaign](research/COLD_QUERY_RESULTS.md).

For the selected cold payload fixtures, client costs were about **6.6–6.9 ms
setup CPU, 0.24–0.71 ms online CPU, and 30 MB peak process memory**, including the
Python harness. Selected metadata downloads were about 3–44 KB. These are desktop
measurements, not a mobile qualification. The directory is reusable for the same
generation and is not tied to a particular search value.

## Repeated queries and generation lifetime

Reuse is separate from query novelty: a warm client can ask an entirely new tag
or value. Compare the same directory layout with Dense and SinglePass, including
setup once per client and rebuild/update costs for each generation.

In the smaller 1,024/4,096-row payload fixtures, Dense beat SinglePass across the
tested 256-query sessions, including setup. Group-1 service averages were
0.14–0.47 ms for Dense and 0.22–1.72 ms for SinglePass. Group 16 was often worse
for both when results were wide. This does not establish a universal warm winner:
session length, data size and client setup limits matter.

The larger **1,024-query sessions** measured the following. Service averages
include setup once per client. Full-campaign values additionally charge measured
generation construction, publication and residual native process work across
the two clients (2,048 answers), including witness construction where applicable.

| Use case / rows | Best tested Dense group / service ms | Best tested SinglePass group / service ms | Full-campaign CPU per answer, Dense / SP ms | Decision |
|---|---:|---:|---:|---|
| Receipt / 10,000 | 1 / 0.247 | 1 / 0.271 | 0.341 / 0.364 | Dense retains a small advantage over this session |
| Document / 65,536 | 16 / 1.634 | 1 / **1.127** | 2.187 / **1.811** | SinglePass is a viable long-session option on a stable generation |
| Secondary index / 65,536 | 1 / **0.767** | 1 / 1.232 | **1.037** / 1.503 | Keep indexed Dense for the tested match count |
| Routing and logs / 65,536 | Qualified | Setup-download gate | See measurements | SinglePass exceeds the 64 MiB initial-download cap in these wider fixtures |
| Precomputed witness / 8,192 | 1 / 1.903 | 1 / **1.516** | 22.259 / 21.860 | Small snapshot-lifetime advantage; not a live-root recommendation |

The document SinglePass option downloads **37.86 MB** initially versus **43.83 KB**
for Dense group 16. Its measured client setup/online CPU was 239 ms / 2.61 ms,
with 45 MB peak process memory; Dense measured 6.83 ms / 0.32 ms and 31.5 MB.
Both fit the configured limits, but mobile bandwidth and generation lifetime
must justify that bulk setup. Warm layouts tested groups 1 and 16 for payloads;
these are measured choices, not a proof of globally optimal parameters.

SinglePass incremental updates were not measured in this snapshot-reuse campaign.
The public directory can also be cached, and does not need SinglePass hints to
remain useful for unrelated new queries.

Machine-readable results include whole-campaign server CPU as well as
request-phase CPU. Charge all publisher and replica work over the actual number
of clients and queries in a generation. Do not interpret a one-time build cost
as free, or use the old arithmetic G projections as a measured fleet crossover.

## Mizu current-root witnesses

**Keep the active base/delta predecessor and node-serving design with Dense.**
The new directory experiment is an immutable-snapshot candidate, not a validated
replacement for live current-root maintenance.

The pilot uses **8,192 values plus a sentinel**, original depth-20 Poseidon roots
and unchanged 2,008-byte witnesses. Every returned witness is verified; tampered
witnesses and wrong roots are rejected. Directory/Dense group 4 measured **2.38 ms
serving CPU** and about **7.61 ms client query/verification CPU** for fresh clients.
Group 1 was better over 64 queries (1.90 ms service per answer).

Constructing the precomputed witness corpus took **40.7 seconds CPU**. Physical
positions are sorted and values are u64s embedded in the field. This does not
measure a live insertion-ordered Shieldd corpus, incremental witness maintenance
or publication on every block. The older approximately 1M-leaf active-generation
benchmark is a different layout/workload; no speed ratio between them is valid.

## Mizu, Shinzo and DefraDB epoch alerts

With database disclosure acceptable, prefer **one common public presence bitmap
per epoch**, then check subscriptions locally. For the tested 65,536-bucket format
this is **8 KiB raw, about 10.98 KB with harness framing**. No selected bucket is
sent to a server. All clients must receive the common epoch artifact on a
query-independent schedule.

At 16,384 inserted fixture keys, one fresh-client check cost **0.18 ms provider
CPU**, versus 0.36 ms for packed Dense and 0.37 ms for directory/Dense. Query
upload and response are zero after public bitmap download; 256 checks reuse the
same bitmap. Its initial download is larger than one packed-PIR poll.

These are equivalent **bucket presence hints**, including collisions, not event
payloads. Matching actions, logs and changed documents still need private
retrieval. Epoch construction, authentication, distribution bandwidth and hit
retrieval are not free. Registration, polling and follow-up timing remain
separate privacy concerns. Keep packed Dense as a bandwidth/serving alternative;
no new GPU concurrency comparison was run here. Immediate DPF remains an option
only for a genuine sub-epoch requirement.

## Evidence boundaries and delivery

- Both replicas must not collude. Processes on one host are not independent
  operators. Query content protection does not hide origin, timing or counts.
- Cold means a fresh client here, not cold disk/CPU caches. Product catch-up and
  ad hoc search can use cached public directories.
- Native request timers include decoding/evaluation/response serialization.
  Input-line reading and startup/cleanup outside those timers are retained in
  whole-process totals. The legacy build/publication field includes that
  residual; it is not a pure generation-build measurement.
- Source index cap: 64 MiB. Client caps: 64 MiB setup download/state, 128 MiB RSS,
  1 MiB per-direction online traffic, 1 s online CPU. Gates are harness boundaries,
  not proof a protocol cannot run with larger budgets.
- No new ratios against 100 decoys were measured. Older GPU projections, large
  logical workloads and weaker-privacy decoy controls remain in
  [BENCHMARKS.md](BENCHMARKS.md); do not combine them with these CPU figures.
- Runtime defaults and production integrations are unchanged. This campaign
  validates research compositions and identifies implementation choices.

Validation: **152 attempted configurations, five repetitions each; 142
result-bearing configurations, 271,070 answers verified, and 50 case repetitions retained explicit
preflight failures.** Result-bearing configurations can still fail client caps;
they are excluded from qualified selections. All **12 regression tests passed**,
including native Dense/SinglePass and finite-encoding integration checks.

[Measurements and reproduction](research/INDEXED_USE_CASE_MEASUREMENTS.md) ·
[Use cases](USE_CASES.md) · [Mechanisms](PROTOCOLS.md) ·
[Integration contract](PRODUCTION.md) · [Roadmap](ROADMAP.md) ·
[Origin and timing privacy](PRIVACY.md)
