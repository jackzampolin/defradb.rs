# InsPIRe GPU same-hardware adapter

This adapter pins the Ethereum-oriented
[`keewoolee/inspire-gpu`](https://github.com/keewoolee/inspire-gpu) server at
commit `c14d1d84a425cdaa9f86ed09465b09c9c9802f13`. It is separate from the
official IEEE S&P CPU artifact in `../inspire_adapter`.

```bash
cd /mnt/c/src/defradb.rs
bash tools/pir-poc/research/run-inspire-gpu-defra.sh quick
bash tools/pir-poc/research/run-inspire-gpu-defra.sh full
```

The checked benchmark-only patch does not change InsPIRe parameters or
cryptographic kernels. It adds:

- the same deterministic 120-byte logical records used by the Dense/GPU-DPF
  adapter, packed into InsPIRe's 64 15-bit slots;
- separate host materialization, GPU preprocessing, and context-build times;
- a first, unwarmed online query split into client generation, server answer,
  and client extraction;
- a machine-readable report for batches 1, 2, 4, 8, 16, and 32.

The first online server measurement starts only after the snapshot is
preprocessed and resident. It is therefore a cold online request, not a cold
snapshot start. A truly cold snapshot is the sum of the separately reported
materialization, preprocessing, and context phases.

By default, an 8 GB GPU runs only the 1 GiB tier. The runner reserves memory
headroom instead of risking a WSL or host OOM. Set
`DEFRA_INSPIRE_GPU_TIERS="1 4"` only when the device and host have enough free
memory. The published 4 GiB instance needs about 6.44 GB resident before
allowing for CUDA, display, and batch scratch. The JSON includes a
`capacity_blocked` row for every skipped 4/16 GiB tier rather than silently
omitting it.

InsPIRe is single-server computational PIR with server-side preprocessing. It
is not security-equivalent to replicated information-theoretic Dense XOR or
two-server GPU-DPF. Its client is portable CPU-only code and retains no
database-dependent hint; CUDA is a server requirement for this adapter.

Join matching same-GPU rows after both suites have run:

```bash
python3 tools/pir-poc/research/compare_gpu_snapshot.py \
  --dense-dpf target/pir-research-results/gpu-dpf-ce23a06af884ee54300b5bc5fd5350e445f10b0b/snapshot-full.json \
  --inspire target/pir-research-results/inspire-gpu-c14d1d84a425cdaa9f86ed09465b09c9c9802f13/inspire-gpu-full.json \
  --output target/pir-research-results/full-gpu-snapshot.json
```
