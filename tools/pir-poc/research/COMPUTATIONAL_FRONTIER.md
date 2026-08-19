# Single-server computational PIR artifact wave

Status date: 2026-08-18. These protocols are evaluated as a separate security
lane from Defra's replicated, information-theoretic Dense/Fuse candidates.

## Reproducible pins

| Artifact | Official pin | Local artifact gate | Common-corpus adapter |
|---|---|---|---|
| SimplePIR / DoublePIR | `ahenzinger/simplepir` commit `e9020b03bf2872c75b8954e749e32408b5db87ed` | Official correctness tests passed | Complete and correctness checked |
| YPIR | USENIX artifact commit `b9801521301f34502496d694b2ac034857104ebc`, annotated tag `artifact-evaluation`; [Zenodo 13117988](https://doi.org/10.5281/zenodo.13117988), archive MD5 `7a1836864bd54fd3288c7a916619b2da` | Both official end-to-end tests pass on the AVX2 fallback | Complete; minimum-table 70-page physical rows, four exact reconstructions |
| InsPIRe | IEEE S&P 2026 [Zenodo 17361471](https://doi.org/10.5281/zenodo.17361471), `artifact-final.zip` MD5 `bfa9edb2d8403f0dc20830fb40608b78` | Blocked: the official artifact requires AVX-512F and this Ryzen 7 3700X exposes AVX2 only | Implemented and mapping-validated; execution requires an AVX-512 runner |

The official InsPIRe archive has no Git metadata. Its permanent record and
archive checksum are therefore the exact pin. The unrelated third-party
`inspire-rs` crate is not evidence for the paper and is not used by this wave.

## Can the exact 262,144 x 96-byte corpus be used?

Yes for logical values and private page selection, but neither new artifact
accepts one compact 96-byte page as one physical record without adaptation.
That distinction is essential to the comparison.

### YPIR

The artifact's YPIR+SP parameter picker asserts that every physical item has at
least `2048 * 14 = 28,672` bits. A 96-byte page has only 768 bits. Because the
input reader consumes exact 14-bit residues, page groups must also be 14-bit
aligned. The adapter searches eligible groupings and packs 70 pages into one
6,720-byte useful row:

- 262,144 pages become 3,745 populated rows, padded to 4,096;
- the artifact returns a 7,168-byte decoded row (448 bytes are padding);
- the client selects the requested 96-byte page locally;
- every sample checks that page byte-for-byte.

Choosing the arithmetic minimum of 38 pages would create 6,899 rows and the
artifact would round to 8,192, doubling first-dimension work. A 64-page mapping
would truncate 12 bits per row because it is not 14-bit aligned. The 70-page
mapping produces the smallest encoded table (28 MiB) among valid
artifact-compatible groups.

This does not leak which page inside the row was requested because that choice
is made only after the private row result reaches the client. It does increase
server work and response geometry, which the adapter reports in full.

### InsPIRe

The official binary creates random residues and exposes no input-file API. Its
query selects one `(row, interpolation sub-column)` result block. The checked
adapter maps one arbitrary corpus byte to one `p=65535` residue and keeps
complete pages inside blocks. Parameter input is charged at 16 physical bits
per useful byte; the original corpus remains 768 useful bits per page.

For the default `dim0=8192` common-corpus geometry, analytical parameter replay
gives a 64 MiB plaintext coefficient layout and a 2,048-coefficient result
block containing 21 complete pages plus 32 unused coefficients. The artifact
must execute before these values enter a measured Pareto table. The adapter
checks the selected page after official decryption.

## Phase and security accounting

All runners keep these horizons separate:

1. corpus transformation / server layout construction;
2. database-dependent server preprocessing;
3. retained client setup or hint download (zero for YPIR and InsPIRe);
4. client query/key generation;
5. online server answer;
6. client recovery;
7. serialized upload and download;
8. encoded table and protocol-state storage.

Allocated table size is not reported as physical DRAM traffic. A candidate gets
physical bytes only after phase-scoped hardware counters are collected.

The security boundary is equally explicit:

- YPIR and InsPIRe use one server and computational lattice assumptions.
- A compromised/malicious server cannot be bypassed for availability.
- The adapters check correctness but do not add malicious-server integrity.
- Dense/Fuse uses replicated servers and information-theoretic privacy against
  up to `n-1` semi-honest colluding replicas.

Numbers can be compared for engineering cost only after preserving this label;
they cannot support a claim that the security models are identical.

## Existing common-corpus baseline

The pinned SimplePIR and YPIR adapters now both have completed common-corpus
measurements on the same 25,165,824 useful bytes:

| Phase / traffic | SimplePIR | YPIR+SP AVX2 |
|---|---:|---:|
| Server online p50 | 3.499 ms | 80.655 ms |
| Client query | 151.002 ms | 62.311 ms |
| Client recovery | 34.790 ms | 5.332 ms |
| Offline client hint | 20,840,448 B | 0 B |
| Online upload | 481,824 B | 573,440 B |
| Online download | 20,352 B | 24,576 B |
| DB + serialized protocol state | 34,048,896 B | 775,128,152 B |

These values are local artifact measurements, not paper claims. YPIR removes
the hint and cuts measured client online CPU by 63.6%, but its scalar server is
23.05x slower and its measured DB plus serialized offline state is 22.77x
larger. Its common-corpus process peaked at 2,084,840 KiB RSS with zero swap.
An AVX-512 result may improve online server time but would be a distinct
hardware result. InsPIRe remains blocked pending such a runner.

## Runners

```bash
# YPIR; uses the scalar artifact path on this AVX2 runner. Its local timing is
# same-host evidence, not a reproduction of the paper's AVX-512 result.
bash tools/pir-poc/research/run-ypir-defra.sh

# InsPIRe; creates BLOCKED.txt and exits 3 when AVX-512F is absent.
bash tools/pir-poc/research/run-inspire-defra.sh
```

The YPIR artifact appendix says the non-AVX-512 path runs with reduced
performance. The InsPIRe source has unconditional AVX-512 intrinsics, so a
portable port would be a new Defra implementation and must be labeled as such.
