# YPIR common-corpus adapter

This adapter pins the official USENIX Security 2024 artifact revision
`b9801521301f34502496d694b2ac034857104ebc` (`artifact-evaluation`) and runs
the artifact on the populated Defra 262,144 x 96-byte page corpus. The upstream
source is not vendored or patched: the runner copies `main.rs` into the ignored
checkout as a new binary.

```bash
cd /mnt/c/src/defradb.rs
bash tools/pir-poc/research/run-ypir-defra.sh
```

The runner first executes the two official end-to-end correctness paths. Use
`--skip-smoke` only after those tests have passed at the pinned revision on the
same machine. `DEFRA_PIR_SAMPLES`, `DEFRA_PIR_ARTIFACT_DIR`,
`DEFRA_PIR_CORPUS_DIR`, and `DEFRA_PIR_RESULT_DIR` override defaults.

## Exact page mapping

The official YPIR+SP parameter picker requires a physical item to contain at
least `2048 * 14` bits. It therefore cannot directly represent one 768-bit
Defra page as one item. The adapter searches 14-bit-aligned page groupings and
minimizes the encoded plaintext table. For the standard corpus, the selected
mapping packs 70 consecutive pages into a 6,720-byte useful row:

- 262,144 pages become 3,745 populated physical rows, padded to 4,096;
- the official parameter picker returns 7,168 bytes, so the row carries 448
  zero padding bytes and no useful page is split or truncated;
- one private physical-row query followed by local slicing returns one exact
  96-byte page.

The adapter pads the final incomplete physical row in memory before handing it
to the artifact's fixed-row `FilePtIter`; this prevents `read_exact` from
discarding a partial final 14-byte input chunk. After one excluded,
correctness-checked warmup, the first measured sample targets the final logical
page to exercise this boundary.

Every measured sample checks the selected page byte-for-byte. The physical
result is nevertheless 7 KiB, and the report charges the full server query,
upload, and response. It does not mislabel the 96 useful bytes as the physical
artifact workload.

## Accounting and security boundary

The JSON keeps corpus/server construction, database-dependent server
preprocessing, client query generation, server online work, and client
recovery separate. YPIR has no client hint. Allocated table bytes are not
hardware memory-traffic measurements.

This is single-server computational PIR under the artifact's lattice
assumptions. It is a separate security lane from the replicated Dense/Fuse
information-theoretic designs and must not be declared security-equivalent.

On AVX-512 the runner enables the artifact's explicit kernel. On other x86-64
machines it uses the artifact's scalar/non-explicit fallback. That result is a
valid same-host AVX2 engineering measurement if correctness passes, but it is
not an AVX-512 paper-result reproduction.
