# Indexed Dense across use cases: measured results

152 attempted configurations; 142 result-bearing configurations; 271,070 verified answers; 50 case repetitions rejected before results.

Serving values are aggregate request-phase CPU milliseconds across replicas and the public metadata provider. Cold includes fresh-client setup delivery, but global build/publication is separate. Session averages charge that client setup once. These are synthetic 64-bit lookup keys and fixed payload projections, not imported production corpora. Bytes include the harness JSON/hex/base64 framing.

## Fresh clients: best qualified directory versus matched controls

| Workload | Source rows | Payload B / matches per key | Directory group | Directory CPU | XOR CPU | Hashed pages CPU | Build/publish + residual ms directory / XOR | Directory setup KB | Upload / download KB | Client setup / online ms | Client peak MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| defra-document | 4,096 | 256 / 1 | 4 | 0.565 | 0.616 | 1.440 | 65.2 / 121.1 | 11.06 | 0.59 / 4.59 | 6.73 / 0.26 | 29.67 |
| defra-document | 65,536 | 256 / 1 | 16 | 2.294 | 4.870 | 6.717 | 1093.1 / 2303.8 | 43.83 | 2.13 / 17.70 | 6.81 / 0.52 | 30.05 |
| defra-secondary | 4,096 | 120 / 16 | 1 | 0.535 | 0.575 | 2.372 | 33.1 / 44.6 | 2.86 | 0.21 / 8.99 | 6.57 / 0.24 | 29.62 |
| defra-secondary | 65,536 | 120 / 16 | 1 | 1.510 | 1.720 | 40.398 | 512.2 / 712.8 | 43.82 | 2.13 / 8.99 | 6.79 / 0.49 | 30.04 |
| global-document | 32,768 | 256 / 1 | 16 | 1.514 | 2.603 | 3.939 | 521.5 / 1055.3 | 21.98 | 1.11 / 17.70 | 6.76 / 0.39 | 29.79 |
| global-receipt | 32,768 | 184 / 1 | 16 | 1.250 | 2.308 | 3.071 | 416.3 / 880.1 | 21.98 | 1.11 / 13.09 | 6.70 / 0.37 | 29.84 |
| global-secondary | 32,768 | 120 / 64 | 1 | 1.385 | 1.552 | 58.866 | 241.8 / 327.0 | 5.59 | 0.34 / 35.30 | 6.57 / 0.37 | 29.65 |
| mizu-routing | 4,096 | 804 / 4 | 1 | 0.911 | 1.071 | 5.533 | 160.2 / 237.6 | 11.06 | 0.59 / 13.36 | 6.65 / 0.29 | 29.67 |
| mizu-routing | 65,536 | 804 / 4 | 4 | 5.140 | gated | gated | 2737.9 / gated | 43.83 | 2.13 / 52.77 | 6.83 / 0.71 | 30.07 |
| shinzo-logs | 4,096 | 548 / 4 | 1 | 0.748 | 0.852 | 3.976 | 115.3 / 165.4 | 11.06 | 0.59 / 9.27 | 6.70 / 0.28 | 29.68 |
| shinzo-logs | 65,536 | 548 / 4 | 4 | 3.939 | 4.984 | gated | 1928.8 / 3059.5 | 43.82 | 2.13 / 36.39 | 6.88 / 0.58 | 30.03 |
| shinzo-receipt | 1,024 | 184 / 1 | 1 | 0.366 | 0.356 | 0.997 | 17.6 / 27.7 | 11.05 | 0.59 / 1.03 | 6.81 / 0.25 | 29.65 |
| shinzo-receipt | 10,000 | 184 / 1 | 4 | 0.722 | 0.917 | 1.877 | 127.8 / 255.5 | 26.80 | 1.33 / 3.44 | 6.75 / 0.38 | 29.85 |
| skewed-secondary | 16,384 | — | — | no qualified directory | — | — | — | — | — | — | — |

## Reused clients: matched directory layouts

Each entry reports setup-amortized service CPU / online-only server CPU per answer. Online-only is measured inside the session and excludes its setup; it is not a fresh-query figure.

| Workload | Rows | Group | Queries/client | Dense ms | SinglePass ms | Full campaign CPU/answer Dense / SP ms | Dense / SP setup download MB |
|---|---:|---:|---:|---:|---:|---:|---:|
| defra-document | 4,096 | 1 | 256 | 0.188 / 0.187 | 0.429 / 0.206 | 0.357 / 0.594 | 0.044 / 2.367 |
| defra-document | 4,096 | 16 | 256 | 0.480 / 0.479 | 1.644 / 1.468 | 0.619 / 1.783 | 0.003 / 2.245 |
| defra-document | 65,536 | 1 | 1024 | 3.082 / 3.081 | 1.127 / 0.241 | 3.795 / 1.811 | 0.699 / 37.864 |
| defra-document | 65,536 | 16 | 1024 | 1.634 / 1.634 | 2.264 / 1.510 | 2.187 / 2.819 | 0.044 / 35.913 |
| defra-secondary | 4,096 | 1 | 256 | 0.299 / 0.299 | 0.901 / 0.805 | 0.382 / 0.984 | 0.003 / 1.131 |
| defra-secondary | 4,096 | 16 | 256 | 2.948 / 2.947 | 11.262 / 11.174 | 3.032 / 11.351 | 0.000 / 1.123 |
| defra-secondary | 65,536 | 1 | 1024 | 0.767 / 0.767 | 1.232 / 0.849 | 1.037 / 1.503 | 0.044 / 18.087 |
| defra-secondary | 65,536 | 16 | 1024 | 3.283 / 3.283 | 11.519 / 11.169 | 3.544 / 11.786 | 0.003 / 17.965 |
| mizu-canonical-witness | 8,192 | 1 | 64 | 1.900 / 1.896 | 12.297 / 0.806 | 326.831 / 337.271 | 0.088 / 33.442 |
| mizu-canonical-witness | 8,192 | 1 | 1024 | 1.903 / 1.902 | 1.516 / 0.810 | 22.259 / 21.860 | 0.088 / 33.442 |
| mizu-canonical-witness | 8,192 | 4 | 64 | 2.008 / 2.005 | 14.426 (unqualified) / 2.745 | 326.603 / 339.165 | 0.022 / 33.259 |
| mizu-canonical-witness | 8,192 | 16 | 64 | 3.837 / 3.834 | 20.713 (unqualified) / 10.278 | 328.472 / 345.517 | 0.006 / 33.259 |
| mizu-routing | 4,096 | 1 | 256 | 0.470 / 0.469 | 1.721 / 1.131 | 0.805 / 2.053 | 0.011 / 6.758 |
| mizu-routing | 4,096 | 16 | 256 | 4.371 / 4.371 | 17.079 / 16.563 | 4.695 / 17.402 | 0.001 / 6.728 |
| mizu-routing | 65,536 | 1 | 1024 | 4.466 / 4.466 | gated | 5.903 / — | 0.175 / — |
| mizu-routing | 65,536 | 16 | 1024 | 7.431 / 7.431 | gated | 8.788 / — | 0.011 / — |
| shinzo-logs | 4,096 | 1 | 256 | 0.356 / 0.356 | 1.230 / 0.823 | 0.596 / 1.476 | 0.011 / 4.661 |
| shinzo-logs | 4,096 | 16 | 256 | 3.055 / 3.055 | 11.786 / 11.427 | 3.288 / 12.021 | 0.001 / 4.631 |
| shinzo-logs | 65,536 | 1 | 1024 | 3.540 / 3.539 | gated | 4.554 / — | 0.175 / — |
| shinzo-logs | 65,536 | 16 | 1024 | 5.318 / 5.317 | gated | 6.281 / — | 0.011 / — |
| shinzo-receipt | 1,024 | 1 | 256 | 0.135 / 0.135 | 0.217 / 0.178 | 0.189 / 0.271 | 0.011 / 0.444 |
| shinzo-receipt | 1,024 | 16 | 256 | 0.365 / 0.365 | 1.144 / 1.110 | 0.414 / 1.192 | 0.001 / 0.414 |
| shinzo-receipt | 10,000 | 1 | 1024 | 0.247 / 0.247 | 0.271 / 0.180 | 0.341 / 0.364 | 0.107 / 4.338 |
| shinzo-receipt | 10,000 | 16 | 1024 | 0.419 / 0.419 | 1.191 / 1.109 | 0.495 / 1.270 | 0.007 / 4.040 |

## Canonical witnesses and epoch presence

| Workload | Rows | Family | Backend | Group | Queries/client | Service CPU ms | Client online ms | Setup KB | Online up/down KB | Cap failures |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 1 | 1 | 2.506 | 7.976 | 87.61 | 4.18 / 8.33 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 4 | 1 | 2.375 | 7.611 | 22.07 | 1.11 / 32.63 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 16 | 1 | 4.232 | 7.793 | 5.69 | 0.34 / 129.83 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 1 | 1 | 734.477 | 7.856 | 33442.17 | 0.13 / 32.77 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 4 | 1 | 746.952 | 7.768 | 33259.20 | 0.12 / 129.96 | client-rss |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 16 | 1 | 677.783 | 8.785 | 33258.96 | 0.12 / 518.76 | client-rss |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 1 | 64 | 1.900 | 7.355 | 87.61 | 4.18 / 8.33 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 4 | 64 | 2.008 | 7.216 | 22.07 | 1.11 / 32.63 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 16 | 64 | 3.837 | 7.440 | 5.69 | 0.34 / 129.83 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 1 | 64 | 12.297 | 7.257 | 33442.18 | 0.13 / 32.77 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 4 | 64 | 14.426 | 7.296 | 33259.19 | 0.12 / 129.96 | client-rss |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 16 | 64 | 20.713 | 8.355 | 33258.96 | 0.12 / 518.76 | client-rss |
| mizu-canonical-witness | 8,192 | canonical-directory | dense | 1 | 1024 | 1.903 | 7.524 | 87.61 | 4.18 / 8.34 | none |
| mizu-canonical-witness | 8,192 | canonical-directory | singlepass | 1 | 1024 | 1.516 | 7.281 | 33442.17 | 0.13 / 32.77 | none |
| shared-epoch-alerts | 1,024 | directory-presence | dense | 16 | 1 | 0.325 | 0.181 | 0.81 | 0.11 / 1.38 | none |
| shared-epoch-alerts | 1,024 | packed-presence | dense | 16 | 1 | 0.368 | 0.171 | 0.05 | 4.18 / 0.21 | none |
| shared-epoch-alerts | 1,024 | public-presence | dense | 16 | 1 | 0.176 | 0.024 | 10.98 | 0.00 / 0.00 | none |
| shared-epoch-alerts | 1,024 | directory-presence | dense | 16 | 256 | 0.133 | 0.135 | 0.81 | 0.11 / 1.38 | none |
| shared-epoch-alerts | 1,024 | packed-presence | dense | 16 | 256 | 0.148 | 0.132 | 0.05 | 4.18 / 0.21 | none |
| shared-epoch-alerts | 1,024 | public-presence | dense | 16 | 256 | 0.001 | 0.019 | 10.98 | 0.00 / 0.00 | none |
| shared-epoch-alerts | 16,384 | directory-presence | dense | 16 | 1 | 0.374 | 0.236 | 9.79 | 0.54 / 1.37 | none |
| shared-epoch-alerts | 16,384 | packed-presence | dense | 16 | 1 | 0.359 | 0.171 | 0.05 | 4.18 / 0.21 | none |
| shared-epoch-alerts | 16,384 | public-presence | dense | 16 | 1 | 0.183 | 0.024 | 10.98 | 0.00 / 0.00 | none |
| shared-epoch-alerts | 16,384 | directory-presence | dense | 16 | 256 | 0.144 | 0.162 | 9.79 | 0.54 / 1.38 | none |
| shared-epoch-alerts | 16,384 | packed-presence | dense | 16 | 256 | 0.147 | 0.132 | 0.05 | 4.18 / 0.21 | none |
| shared-epoch-alerts | 16,384 | public-presence | dense | 16 | 256 | 0.001 | 0.019 | 10.98 | 0.00 / 0.00 | none |

## Reproduction and limits

Run `run_indexed_use_cases.py --output NEW_DIR --native NATIVE_STORE --bridge COLD_CANONICAL --repeats 5` from this directory in Linux/WSL. Then run `report_indexed_use_cases.py NEW_DIR --output OUTPUT_PREFIX`. The cold-search runner freezes source modules and hashes the native binary. The root contains the exact matrix, canonical corpus and wrong-root/tamper checks; each case retains its raw process phases and client measurements.

Add `--profile large-warm` with a new output directory for the 1,024-query larger-scope sessions. Use `--additional LARGE_WARM_ROOT` when generating this combined report.

The `witness-warm` profile takes `--canonical-corpus MAIN_ROOT/canonical-8192.json` and reuses that measured snapshot for 1,024-query witness sessions. Include its root with another `--additional`.

- Five repetitions alternate execution order over the same deterministic corpus for each shape. Client processes are fresh and sequential; no claim of a load-tested fleet.
- Payload widths are benchmark projections (804/548/184/256/120 B), plus record framing. Tags have uniform multiplicity except the explicitly hot-value case (2,048 records).
- Complete all-match retrieval is verified, including absent values. No result-driven extra network requests. A large hot group can force every answer over the wire cap.
- Global fixtures search their entire stated scope. Their smaller resident sizes do not validate million/billion-row deployments or arbitrary compound predicates.
- Canonical witnesses preserve the existing Poseidon depth-20 root and 2,008-byte witness. The fixture has 8,192 values plus the sentinel, sorted physical positions; live updates/root maintenance remain unmeasured.
- The canonical corpus builder used 40.7 seconds CPU once for this snapshot. This is included once per generation in the machine-readable totals. Precomputed full witnesses are not a measured replacement for the existing active base/delta predecessor and node-plane serving design.
- Alert controls answer the same 65,536-bucket presence hint, including collisions. Payloads are separate. The public bitmap uses no native PIR endpoint and exposes no selected bucket. It must be distributed to all clients on a query-independent epoch schedule.
- Public bitmap CPU is metadata-provider delivery CPU, not a claim that epoch construction, bandwidth, authentication or payload follow-ups are free.
- Resource limits: 64 MiB logical index; 64 MiB client setup download/state; 128 MiB client RSS; 1 MiB per-direction online wire; 1 s client online CPU. Failures are retained, not dropped from qualification.
- SinglePass uses this repository's show-and-shuffle adapter and four partitions; these are backend/layout comparisons, not a complete retuning of every SinglePass parameter.
- Canonical client RSS adds the parent high-water mark and a conservative child-verifier high-water mark. A failure of this bound is a memory-qualification gate, not proof of simultaneous resident use over the cap.
- Timings include harness serialization. Do not divide these CPU results by historical GPU/decoy wall-time projections. No production serving defaults were changed.

The machine-readable `full_campaign_server_cpu_ms` sums every native process CPU counter plus publisher build/publication/delivery and canonical construction, without dropping work outside request timers. `native_cpu_outside_request_phases_ms` exposes that residual (including input-line reading, startup and cleanup). The legacy `global_publish_build_cpu_ms` includes this residual; it is not a pure generation-build measurement. Do not treat its G projections as an independently measured generation-lifetime crossover.
