# InsPIRe official-artifact common-corpus adapter

The official public artifact is the permanent IEEE S&P 2026 Zenodo record
[`17361471`](https://doi.org/10.5281/zenodo.17361471). It contains no Git
metadata, so the reproducible pin is the record plus
`artifact-final.zip` MD5 `bfa9edb2d8403f0dc20830fb40608b78`. This is distinct
from the third-party `inspire-rs` crate and supersedes that crate as evidence
for the paper.

```bash
cd /mnt/c/src/defradb.rs
bash tools/pir-poc/research/run-inspire-defra.sh
```

The runner restores the exact archive on every invocation and applies a
checked, out-of-tree input patch. The cryptographic kernels and parameter
selection are unchanged. It first runs the artifact's smallest official
end-to-end smoke case, then runs the common corpus and writes both the original
measurement and a qualified aggregate-work report. `DEFRA_INSPIRE_DIM0`
(default 8192), `DEFRA_PIR_SAMPLES`, `DEFRA_PIR_ARTIFACT_DIR`,
`DEFRA_PIR_CORPUS_DIR`, and `DEFRA_PIR_RESULT_DIR` are configurable.

## Exact page mapping

The official binary generates random plaintext residues and has no file-input
API. A private query chooses a `(row, interpolation sub-column)` block and
returns `c * gamma` plaintext coefficients. The adapter:

1. uses one arbitrary page byte per plaintext residue under `p = 65535`;
2. sizes the physical input at 16 bits per byte while retaining the original
   768 useful bits per page in the comparison report;
3. places complete pages inside result blocks so a page never crosses a query;
4. selects one block privately, decrypts it through the official protocol, and
   checks the requested 96-byte page byte-for-byte.

This preserves the logical page value and private selection, but it does not
pretend that the artifact operates on one compact 96-byte physical record. The
report charges the expanded table, the whole private result block, all query
keys, and the encrypted response.

## Hardware and security boundary

The artifact imports and invokes AVX-512 intrinsics unconditionally and compiles
with `target-cpu=native`. The runner exits with a durable `BLOCKED.txt` before
building if `avx512f` is absent. No scalar number or paper number is substituted
for a local measurement.

InsPIRe is single-server computational PIR with server-side preprocessing. It
does not have the replicated information-theoretic privacy of the Dense/Fuse
line. The qualified report keeps encoding, preprocessing, online server time,
client query/decode time, and traffic separate.
