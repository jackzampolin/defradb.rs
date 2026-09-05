# Reproducing the cold-search pass

Run in Linux/WSL from `tools/pir-poc/research`. The reference run used Python 3.14,
NumPy 2.3.5, Rust/Go, about 7.7 GiB RAM and an RTX 2070 SUPER. Output directories
must be new. Do not run timed campaigns alongside each other or compilation.
The orchestration scripts used for this particular host contain its absolute
artifact paths; the general runner accepts explicit paths.

## Native controls and Python checks

```bash
python3 prepare_index_compositions.py /tmp/pir-cold-repro-source \
  --target-dir /tmp/pir-cold-repro-build

PIR_COLD_NATIVE=/tmp/pir-cold-repro-build/release/examples/native_store \
  python3 -m unittest test_cold_search test_cold_layouts test_cold_segmented -v

python3 run_cold_search.py --profile smoke --repeats 1 \
  --native /tmp/pir-cold-repro-build/release/examples/native_store \
  --output /tmp/pir-cold-repro-smoke
```

For actual finite integration checks, also set `PIR_COLD_FINITE` to the finite
store binary. Without it the corresponding integration test is explicitly skipped.

General profiles are `screen`, `finite`, `directory`, `extensions`, `frontier`,
`bit64` and `reuse`. Use `--repeats 5`. For an exact previous matrix, use
`--matrix-from <manifest-or-matrix.json>`; optional `--clients 4 --repeats 1`
provides a separate qualification run without changing old results. `--resume`
requires the same manifest and refuses to overwrite failed cases.

The runner copies its Python modules into each campaign's `source/` directory;
case processes import that frozen copy. Native executable hashes, parameters,
results, cap failures, timeouts and logs are retained. A correctness failure is
not a performance sample.

## Finite-differences store

Check out revision `4574a4f8c52eeda165e110cbb64f834397d7c049` of the
[reference](https://github.com/ahenzinger/finite-diffs-pir). Copy
`benchmarks/finite_store.go` into its `pir/` package and
`benchmarks/finite_fast_test.go` as `pir/cold_fast_test.go`. Add
`cmd/cold-store/main.go` containing:

```go
package main
import p "github.com/ahenzinger/finite-diffs-pir/pir"
func main() { p.ColdServe() }
```

From that checkout:

```bash
go test ./pir -run TestColdFastEncodingEqualsReference -count=1
go build -o /tmp/pir-finite-store ./cmd/cold-store
```

Supply `--finite /tmp/pir-finite-store` to the general runner. Default parameters
use the author's original encoder and a 256 MiB/replica preflight. Explicit
`finite_m`/`finite_d` configurations use the tested in-place encoder and a larger
bounded allocation. The saved larger-memory matrix contains the exact settings;
it can require approximately 5 GiB encoded storage across the two roles.

## Canonical witnesses

From the repository root:

```bash
cargo build --locked --release -p pir-poc --features research --example cold-canonical
```

Then from the research directory:

```bash
python3 run_cold_canonical.py \
  --bridge ../../../target/release/examples/cold-canonical \
  --native /tmp/pir-cold-repro-build/release/examples/native_store \
  --output /tmp/pir-cold-canonical-repro
```

The bridge/native paths are resolved before creating the frozen matrix, because
case working directories are the campaign snapshots. The canonical
bridge builds small fixtures using the existing Poseidon code. It preserves
original witness/root bytes; it does not export a production Shieldd database.

## Other lanes

- `run_cold_ramen.py`: persistent bridge from the native-control build.
- `run_cold_maintenance.py`: public updates plus real private base/delta reads.
- `prepare_native_batch.py`: generates the separate native batch example from
  `native_store.rs`; build it as `oram/examples/native_batch.rs`, then use
  `run_dense_batch.py`.
- `screen_cold_frontiers.py --kernel`: exact CRT kernels and resource gates.
- `patch_sandwich_batch.py`: apply once to the pinned Sandwich HTTP wrapper,
  then build `pir_server` with CUDA and `pir-client` with its `native` CLI feature.
  `run_sandwich_batch.py` accepts `--clients`, `--window`, and `--spacing`.
- `prepare_zippir.py`: documented CPU compatibility and measurement patch;
  configure with CMake's `-DCMAKE_POLICY_VERSION_MINIMUM=3.5`, build Release and
  run full setup with `OMP_NUM_THREADS=1`. Do not use `--online_only` for admission.
- `benchmarks/hintless_cold_test.inc`: append once to the pinned Hintless test
  translation unit. Build `//hintless_simplepir:hintless_simplepir_test` with
  Bazel 7.4.1, `-c opt --enable_workspace=false`; run only
  `HintlessColdSearch.CompleteTagRecords` and set `PIR_COLD_HINTLESS_OUTPUT`.

Exact CUDA/CUTLASS pins and experiment boundaries are in the
[execution ledger](COLD_QUERY_EXECUTION.md). HE tests and cost kernels are not
automatically matched complete-search benchmarks.

## Summaries

```bash
python3 analyze_cold_search.py /tmp/pir-cold-repro-smoke \
  --output /tmp/pir-cold-summary
```

This emits CSV and JSON. It separates client CPU, aggregate service CPU, global
CPU, memory and wire; it does not hide failed caps or merge unlike protocols into
one ranking. The canonical corpus builder is charged once to its generation.
`analyze_cold_artifacts.py` provides the separate host-specific GPU/batch summary.
