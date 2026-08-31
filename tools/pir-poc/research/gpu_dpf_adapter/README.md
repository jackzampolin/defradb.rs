# GPU Dense and DPF research adapter

This adapter compares two strict two-server snapshot protocols on one
GPU-resident table:

- bit-packed replicated Dense XOR, used as the simple memory-bandwidth control;
- the pinned
  [`facebookresearch/GPU-DPF`](https://github.com/facebookresearch/GPU-DPF)
  ChaCha12 construction and fused DPF-expansion/table-reduction kernel.

It is research-only and changes no DefraDB storage, query, CRDT, or API code.
The upstream repository is pinned to commit
`ce23a06af884ee54300b5bc5fd5350e445f10b0b`. It is archived, labels itself a
proof of concept, and must not be treated as a production-vetted cryptographic
library.

Run from WSL with a CUDA toolkit and NVIDIA GPU available:

```bash
bash tools/pir-poc/research/run-gpu-pir-defra.sh quick
bash tools/pir-poc/research/run-gpu-pir-defra.sh full
```

The runner defaults to `/opt/cuda-12.4`; override `DEFRA_CUDA_HOME` and
`DEFRA_CUDA_ARCH` when necessary. It clones the exact upstream commit outside
the repository and writes generated binaries and JSON under `target/`.

## Snapshot scope

Both protocols operate on the same deterministic table and every reconstructed
answer is checked. A useful row is 120 bytes, matching the published
`inspire-gpu` benchmark. The upstream arithmetic kernel operates on 128-bit
limbs, so the physical GPU row is padded to 128 bytes; upload and response
accounting still reports the 120 useful bytes.

For each logical query:

- Dense creates two fresh bit-vector shares, one bit per row per replica;
- DPF creates two compact upstream keys;
- the two replicas execute sequentially on this one benchmark GPU;
- aggregate server time is their sum;
- parallel wall time is the larger replica time, modeling two equal independent
  GPU servers without claiming that this host is non-colluding.

The benchmark separates client key/share generation, host-to-device query copy,
GPU kernel time, device-to-host response copy, wire bytes, and approximate NVML
power. HTTP, TLS, queueing, persistent-table loading, network transfer and
keyword-to-ordinal mapping are excluded.

Schema v3 also records protocol-context construction and the first H2D,
unwarmed answer and D2H phases before calibration. Dense/DPF first-online
results are measured; the live packed-presence control currently labels that
optional object `measured: false`. GPU table materialization uses the synthetic
deterministic initialization kernel and is not a cold-storage/file-load time.

The full snapshot matrix runs `2^20`, `2^23`, and `2^25` rows at batches 1, 8,
32, and 128. `2^25 x 128` is the largest case that fits this POC's 8 GB GPU once
the 4 GiB table, selectors, DPF state and answers coexist. On a device with at
least 30 GiB, the runner also schedules the `2^27`/16 GiB tier; otherwise the
suite emits an explicit `capacity_blocked` record.

## Live epoch-histogram scope

The live executable tests a deliberately simpler subscription schedule than
one DPF evaluation per subscription per event:

1. events in a fixed public epoch set bits in a fixed 65,536-bucket presence
   bitmap (the 16-byte histogram remains as an intentionally overprovisioned
   control);
2. every subscriber registers one private selector/key pair that can be reused
   across epochs;
3. servers batch the private presence reads;
4. the client reconstructs a fixed hit bit and, on a hit,
   performs a separate private snapshot fetch for the epoch.

The strict alternatives are full-row Dense, Compact DPF, and a specialized
packed-presence Dense parity kernel. A one-server 100-visible-bucket CPU lookup
is the matched weaker-privacy control. This hides the subscribed bucket only on
the strict paths and deliberately reveals the epoch cadence and public 16-bit
domain. It reports one fixed response per subscriber per epoch rather than one
response per event. Presence construction is ordinary `O(events)` work and is
outside the timed retrieval kernel.

Packed presence is exact even if a bucket occurs multiple times: building the
bitmap uses OR, and PIR retrieves the final presence bit. It requires 8 KiB of
registered selector state per subscriber per server, sends 8 KiB to each server
once, and returns one answer byte per server per epoch before framing. Compact
DPF reduces stored registration to 2,080 bytes/server at higher PRF-expansion
work. Dense extends directly to three or more XOR-share replicas; this pinned
DPF implementation is exactly two-party.

## Interpretation boundary

GPU latency is not a hardware-independent measure of computation or cost. The
report includes approximate power samples, but capacity planning still needs
the intended GPU, electricity price, replication topology, concurrency, and
network. GPU DPF has computational PRG security and is currently a two-party
lane; Dense XOR retains information-theoretic privacy and extends directly to
three or more non-colluding replicas.

The recorded local run used CUDA 12.4 with GCC 13 under Ubuntu 26.04/WSL and an
RTX 2070 SUPER. CUDA 12.4 predates glibc 2.42; that combination needs the known
`noexcept(true)` compatibility adjustment for `cospi`, `sinpi`, `rsqrt` and
their float declarations in the isolated CUDA toolkit. Prefer a supported
CUDA/base-image pairing for reproduction and production rather than patching a
system toolkit.
