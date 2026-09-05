# Private index composition measurements

629 completed runs; 66668 verified complete answers; 0 retained failures.

The primary total below sums every server process and the separately timed index construction/setup/update controller, amortized over the actual number of queries. This conservatively includes client-side setup work in the lifecycle term. Online client CPU is reported separately; all-participant CPU is in the CSV. Process startup and rebuild costs are not discarded.

These are local research prototypes with JSON serialization, public dimensions, one honest client/owner, and public writer update schedules. The controller also retains the publisher and correctness oracle; its RSS cap therefore conservatively covers those copies. Query methods receive a metadata-only view. This is not a production ranking or a malicious-security audit.

Rows may only be compared at matching payload width, predicate/output semantics, data layout, padding and lifecycle. Native store controls use a compiled set-bit XOR kernel; Path ORAM still has client-side Python cryptography. Ramen operates on 15-byte limbs. Bitmap cases include three extra MPC processes; wavelet count-only is a separate workload. Authenticated cases use SHA-256 and a trusted fresh root, not production Poseidon witness bytes.

Legacy Ramen bridge phase timings omit response serialization; those rows mark online CPU with `~`. Their full process/lifecycle totals remain measured. Final bridge timings include response serialization. Wall-clock results from overlapping local campaigns are not used for rankings.

| Family | Backend | N / payload B | Variant | Runs | Online server ms | Client p95 ms | Total ms/answer | Caps |
|---|---|---:|---|---:|---:|---:|---:|---|
| radix | dense | 32 / 16 | g=1, leaf=0, Q=12, P=4 | 1 | 1.044 | 0.971 | 12.793 | pass |
| radix | dense | 32 / 16 | g=2, leaf=0, Q=12, P=4 | 1 | 0.675 | 0.707 | 11.658 | pass |
| radix | dense | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.484 | 0.543 | 10.774 | pass |
| radix | dense | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 0.367 | 0.366 | 11.284 | pass |
| radix | path | 32 / 16 | g=1, leaf=0, Q=12, P=4 | 1 | 1.229 | 1.933 | 7.419 | pass |
| radix | path | 32 / 16 | g=2, leaf=0, Q=12, P=4 | 1 | 0.759 | 1.243 | 6.756 | pass |
| radix | path | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.550 | 0.775 | 6.289 | pass |
| radix | path | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 0.599 | 0.870 | 6.708 | pass |
| hash | dense | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.512 | 0.576 | 11.760 | pass |
| hash | path | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.665 | 0.967 | 6.269 | pass |
| hash | singlepass | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.542 | 0.655 | 11.528 | pass |
| bitmap | dense | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 5.329 | 11.874 | 33.319 | pass |
| bitmap | dense | 32 / 16 | g=32, leaf=0, Q=12, P=4 | 1 | 3.160 | 6.757 | 30.724 | pass |
| bitmap | path | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 5.690 | 14.206 | 28.896 | pass |
| bitmap | path | 32 / 16 | g=32, leaf=0, Q=12, P=4 | 1 | 3.260 | 7.584 | 24.317 | pass |
| wavelet | dense | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 4.280 | 3.808 | 14.673 | pass |
| wavelet | dense | 32 / 16 | g=32, leaf=0, Q=12, P=4 | 1 | 4.095 | 3.844 | 14.281 | pass |
| wavelet | path | 32 / 16 | g=8, leaf=0, Q=12, P=4 | 1 | 4.329 | 6.209 | 9.920 | pass |
| wavelet | path | 32 / 16 | g=32, leaf=0, Q=12, P=4 | 1 | 4.299 | 6.169 | 9.793 | pass |
| posting | dense | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.184 | 0.164 | 10.793 | pass |
| posting | path | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.192 | 0.273 | 5.713 | pass |
| posting | singlepass | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 0.173 | 0.237 | 11.011 | pass |
| authenticated | dense | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 3.114 | 2.371 | 14.022 | pass |
| authenticated | path | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 1 | 3.470 | 4.927 | 9.581 | pass |
| authenticated | path | 32 / 16 | g=4, leaf=0, Q=12, P=4, value/4 | 1 | 3.403 | 4.900 | 10.399 | pass |
| authenticated | path | 32 / 16 | g=4, leaf=0, Q=12, P=4, delete/4 | 1 | 3.556 | 5.331 | 10.773 | pass |
| authenticated | path | 32 / 16 | g=4, leaf=0, Q=12, P=4, insert/4 | 1 | 3.755 | 5.283 | 11.323 | pass |
| posting | singlepass | 32 / 32 | g=4, leaf=0, Q=12, P=4, value/4 | 1 | 0.200 | 0.239 | 33.200 | pass |
| bitmap | path | 32 / 32 | g=16, leaf=0, Q=12, P=4, clustered | 1 | 3.423 | 7.863 | 27.160 | pass |
| wavelet | path | 32 / 32 | g=16, leaf=0, Q=12, P=4, count, clustered | 1 | 3.946 | 5.484 | 9.947 | pass |
| radix | dense | 256 / 96 | g=1, leaf=0, Q=32, P=4 | 3 | 2.890 | 1.603 | 7.188 | pass |
| radix | dense | 256 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 1.322 | 0.865 | 5.248 | pass |
| radix | dense | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.748 | 0.541 | 4.738 | pass |
| radix | dense | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 0.536 | 0.376 | 4.492 | pass |
| radix | path | 256 / 96 | g=1, leaf=0, Q=32, P=4 | 3 | 3.083 | 4.367 | 6.515 | pass |
| radix | path | 256 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 1.577 | 2.253 | 4.180 | pass |
| radix | path | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.955 | 1.349 | 3.552 | pass |
| radix | path | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 0.776 | 1.086 | 3.615 | pass |
| hash | dense | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.837 | 0.569 | 4.849 | pass |
| hash | path | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.284 | 1.768 | 4.260 | pass |
| hash | singlepass | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.592 | 0.675 | 4.879 | pass |
| bitmap | dense | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 7.683 | 12.785 | 18.192 | pass |
| bitmap | dense | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 7.787 | 12.850 | 17.863 | pass |
| bitmap | path | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 5.729 | 14.407 | 15.443 | pass |
| bitmap | path | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 5.671 | 14.156 | 15.099 | pass |
| wavelet | dense | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 15.030 | 5.957 | 19.205 | pass |
| wavelet | dense | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 11.643 | 5.902 | 15.972 | pass |
| wavelet | path | 256 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 9.311 | 13.990 | 12.809 | pass |
| wavelet | path | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 8.917 | 13.210 | 11.822 | pass |
| posting | dense | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.323 | 0.197 | 4.383 | pass |
| posting | path | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.329 | 0.501 | 3.421 | pass |
| posting | singlepass | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.186 | 0.215 | 4.270 | pass |
| authenticated | dense | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 15.038 | 3.771 | 19.434 | pass |
| authenticated | path | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 5.817 | 8.946 | 9.820 | pass |
| authenticated | path | 256 / 96 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 5.840 | 8.964 | 17.615 | pass |
| authenticated | path | 256 / 96 | g=4, leaf=0, Q=12, P=4, delete/4 | 3 | 5.758 | 8.964 | 17.258 | pass |
| authenticated | path | 256 / 96 | g=4, leaf=0, Q=12, P=4, insert/4 | 3 | 5.789 | 8.834 | 17.382 | pass |
| posting | singlepass | 256 / 32 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 0.179 | 0.196 | 31.456 | pass |
| bitmap | path | 256 / 32 | g=16, leaf=0, Q=12, P=4, clustered | 3 | 3.377 | 8.132 | 27.357 | pass |
| wavelet | path | 256 / 32 | g=16, leaf=0, Q=12, P=4, count, clustered | 3 | 6.591 | 10.138 | 13.292 | pass |
| radix | dense | 1024 / 96 | g=1, leaf=0, Q=32, P=4 | 1 | 9.230 | 2.060 | 13.770 | pass |
| radix | dense | 1024 / 96 | g=1, leaf=0, Q=32, P=4 | 2 | 9.724 | 2.314 | 15.145 | pass |
| radix | dense | 1024 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 3.730 | 1.189 | 8.515 | pass |
| radix | dense | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 2.047 | 0.746 | 6.321 | pass |
| radix | dense | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 2.305 | 0.635 | 6.862 | pass |
| radix | path | 1024 / 96 | g=1, leaf=0, Q=32, P=4 | 3 | 3.839 | 5.596 | 10.364 | pass |
| radix | path | 1024 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 2.014 | 3.000 | 6.337 | pass |
| radix | path | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.354 | 2.019 | 5.640 | pass |
| radix | path | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 1.755 | 2.310 | 8.622 | pass |
| hash | dense | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 2.436 | 0.627 | 7.030 | pass |
| hash | path | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 2.110 | 2.799 | 10.046 | pass |
| hash | singlepass | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.652 | 0.781 | 5.701 | pass |
| bitmap | dense | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 19.296 | 13.816 | 30.867 | pass |
| bitmap | dense | 1024 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 18.751 | 13.144 | 30.357 | pass |
| bitmap | path | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 5.984 | 14.764 | 20.035 | pass |
| bitmap | path | 1024 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 6.314 | 15.375 | 20.556 | pass |
| wavelet | dense | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 67.282 | 7.657 | 72.332 | pass |
| wavelet | dense | 1024 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 39.472 | 7.359 | 44.158 | pass |
| wavelet | path | 1024 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 12.549 | 19.649 | 20.126 | pass |
| wavelet | path | 1024 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 12.014 | 18.707 | 17.042 | pass |
| posting | dense | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.841 | 0.225 | 5.306 | pass |
| posting | path | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.351 | 0.572 | 6.640 | pass |
| posting | singlepass | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.190 | 0.218 | 4.886 | pass |
| authenticated | dense | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 81.009 | 4.706 | 87.618 | pass |
| authenticated | path | 1024 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 7.442 | 11.890 | 17.020 | pass |
| authenticated | path | 1024 / 96 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 7.957 | 12.595 | 36.383 | pass |
| authenticated | path | 1024 / 96 | g=4, leaf=0, Q=12, P=4, delete/4 | 3 | 8.222 | 12.850 | 36.809 | pass |
| authenticated | path | 1024 / 96 | g=4, leaf=0, Q=12, P=4, insert/4 | 3 | 8.354 | 14.885 | 39.238 | pass |
| posting | singlepass | 1024 / 32 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 0.207 | 0.240 | 36.902 | pass |
| bitmap | path | 1024 / 32 | g=16, leaf=0, Q=12, P=4, clustered | 3 | 3.854 | 8.864 | 41.135 | pass |
| wavelet | path | 1024 / 32 | g=16, leaf=0, Q=12, P=4, count, clustered | 3 | 9.332 | 15.321 | 20.006 | pass |
| radix | dense | 4096 / 96 | g=1, leaf=0, Q=32, P=4 | 3 | 49.342 | 2.977 | 56.372 | pass |
| radix | dense | 4096 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 16.485 | 1.514 | 22.967 | pass |
| radix | dense | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 7.342 | 0.909 | 13.049 | pass |
| radix | dense | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 9.326 | 0.726 | 16.522 | pass |
| radix | path | 4096 / 96 | g=1, leaf=0, Q=32, P=4 | 3 | 5.517 | 8.343 | 28.595 | controller-transient-rss |
| radix | path | 4096 / 96 | g=2, leaf=0, Q=32, P=4 | 3 | 2.884 | 4.305 | 14.917 | pass |
| radix | path | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.620 | 2.414 | 13.585 | pass |
| radix | path | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 2.579 | 3.404 | 31.353 | controller-transient-rss |
| hash | dense | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 9.370 | 0.767 | 16.623 | pass |
| hash | path | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 2.518 | 3.375 | 30.271 | controller-transient-rss |
| hash | singlepass | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.645 | 0.793 | 8.775 | pass |
| bitmap | dense | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 116.163 | 15.158 | 135.013 | pass |
| bitmap | dense | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 116.244 | 15.232 | 134.743 | pass |
| bitmap | path | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 6.440 | 15.679 | 43.903 | controller-transient-rss |
| bitmap | path | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 6.455 | 15.805 | 43.109 | controller-transient-rss |
| wavelet | dense | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 609.635 | 12.397 | 618.372 | pass |
| wavelet | dense | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 245.370 | 12.776 | 251.349 | pass |
| wavelet | path | 4096 / 96 | g=8, leaf=0, Q=32, P=4 | 3 | 17.607 | 30.808 | 47.564 | controller-transient-rss;online-client-wire |
| wavelet | path | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 16.430 | 26.469 | 31.841 | online-client-wire |
| posting | dense | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 3.755 | 0.254 | 10.393 | pass |
| posting | path | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.410 | 0.650 | 22.138 | controller-transient-rss |
| posting | singlepass | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.218 | 0.288 | 7.858 | pass |
| authenticated | dense | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 698.479 | 9.954 | 714.067 | pass |
| authenticated | path | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 9.876 | 16.325 | 44.639 | controller-transient-rss |
| authenticated | path | 4096 / 96 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 9.700 | 16.064 | 106.102 | controller-transient-rss |
| authenticated | path | 4096 / 96 | g=4, leaf=0, Q=12, P=4, delete/4 | 3 | 9.739 | 16.244 | 110.619 | controller-transient-rss |
| authenticated | path | 4096 / 96 | g=4, leaf=0, Q=12, P=4, insert/4 | 3 | 9.367 | 15.713 | 145.700 | controller-transient-rss |
| posting | singlepass | 4096 / 32 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 0.267 | 0.281 | 48.862 | pass |
| bitmap | path | 4096 / 32 | g=16, leaf=0, Q=12, P=4, clustered | 3 | 4.100 | 11.039 | 89.121 | controller-transient-rss |
| wavelet | path | 4096 / 32 | g=16, leaf=0, Q=12, P=4, count, clustered | 3 | 11.628 | 22.578 | 38.572 | pass |
| radix | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~218.852 | 1.073 | 225.142 | pass |
| hash | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~906.351 | 1.379 | 930.904 | pass |
| bitmap | ramen | 32 / 16 | g=16, leaf=0, Q=12, P=4 | 3 | ~747.291 | 15.382 | 789.490 | pass |
| wavelet | ramen | 32 / 16 | g=16, leaf=0, Q=12, P=4 | 3 | ~771.241 | 7.907 | 779.839 | pass |
| posting | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~72.730 | 0.389 | 79.856 | pass |
| authenticated | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~6357.845 | 7.234 | 6461.781 | pass |
| radix | ramen | 128 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~295.829 | 2.350 | 321.175 | pass |
| hash | ramen | 128 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~620.747 | 2.559 | 645.123 | pass |
| bitmap | ramen | 128 / 16 | g=16, leaf=0, Q=12, P=4 | 3 | ~1372.585 | 16.149 | 1483.622 | pass |
| wavelet | ramen | 128 / 16 | g=16, leaf=0, Q=12, P=4 | 3 | ~1479.610 | 9.118 | 1502.371 | pass |
| posting | ramen | 128 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~91.861 | 0.353 | 116.835 | pass |
| authenticated | ramen | 128 / 16 | g=4, leaf=0, Q=12, P=4 | 3 | ~14827.651 | 8.507 | 15312.132 | pass |
| authenticated | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4, value/4 | 3 | 6624.062 | 7.560 | 7374.246 | pass |
| authenticated | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4, delete/4 | 3 | 6532.234 | 7.716 | 7209.531 | pass |
| authenticated | ramen | 32 / 16 | g=4, leaf=0, Q=12, P=4, insert/4 | 3 | 6327.801 | 7.105 | 6979.423 | pass |
| radix | dense-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.433 | 0.435 | 0.730 | pass |
| radix | path-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 3.363 | 1.428 | 4.418 | pass |
| radix | singlepass-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.742 | 0.503 | 1.153 | pass |
| hash | dense-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.561 | 0.498 | 0.919 | pass |
| hash | path-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 6.151 | 2.100 | 7.997 | pass |
| hash | singlepass-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.158 | 0.637 | 1.690 | pass |
| bitmap | dense-native | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 5.605 | 15.218 | 12.620 | pass |
| bitmap | path-native | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 10.138 | 15.976 | 19.362 | pass |
| wavelet | dense-native | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 4.825 | 5.263 | 5.894 | pass |
| wavelet | path-native | 256 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 25.650 | 14.547 | 27.881 | pass |
| posting | dense-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.162 | 0.204 | 0.511 | pass |
| posting | path-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.232 | 0.557 | 2.990 | pass |
| posting | singlepass-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.271 | 0.229 | 0.856 | pass |
| authenticated | dense-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 3.203 | 3.570 | 4.639 | pass |
| authenticated | path-native | 256 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 19.690 | 10.345 | 23.113 | pass |
| radix | dense-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.843 | 0.751 | 3.250 | pass |
| radix | path-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 6.107 | 2.957 | 24.750 | pass |
| radix | singlepass-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.059 | 0.819 | 5.299 | pass |
| hash | dense-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.041 | 0.657 | 5.572 | pass |
| hash | path-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 12.687 | 4.384 | 59.717 | controller-transient-rss |
| hash | singlepass-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.670 | 0.959 | 10.930 | pass |
| bitmap | dense-native | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 7.632 | 14.952 | 25.053 | pass |
| bitmap | path-native | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 11.712 | 17.936 | 59.970 | controller-transient-rss |
| wavelet | dense-native | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 10.992 | 9.098 | 15.351 | pass |
| wavelet | path-native | 4096 / 96 | g=32, leaf=0, Q=32, P=4 | 3 | 46.481 | 29.073 | 66.495 | online-client-wire |
| posting | dense-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.254 | 0.200 | 3.578 | pass |
| posting | path-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 1.626 | 0.847 | 32.664 | controller-transient-rss |
| posting | singlepass-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 0.275 | 0.244 | 7.203 | pass |
| authenticated | dense-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 15.774 | 6.433 | 28.878 | pass |
| authenticated | path-native | 4096 / 96 | g=4, leaf=0, Q=32, P=4 | 3 | 33.391 | 18.518 | 84.995 | controller-transient-rss;online-client-wire |
| radix | dense-native | 256 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 6 | 1.334 | 1.593 | 2.725 | pass |
| radix | dense-native | 256 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 6 | 1.016 | 1.185 | 2.133 | pass |
| radix | dense-native | 256 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 6 | 0.707 | 0.844 | 1.524 | pass |
| radix | path-native | 256 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 6 | 6.267 | 4.101 | 10.355 | pass |
| radix | path-native | 256 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 6 | 4.696 | 2.946 | 8.656 | pass |
| radix | path-native | 256 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 6 | 3.014 | 1.956 | 5.163 | pass |
| radix | singlepass-native | 256 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 6 | 1.608 | 1.550 | 3.694 | pass |
| radix | singlepass-native | 256 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 6 | 1.245 | 1.256 | 2.767 | pass |
| radix | singlepass-native | 256 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 6 | 0.903 | 0.909 | 2.019 | pass |
| radix | ramen | 256 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 3 | 5494.394 | 3.835 | 5845.361 | pass |
| radix | ramen | 256 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 2 | 4252.584 | 2.934 | 4578.854 | pass |
| radix | dense-native | 1024 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 3 | 1.569 | 1.435 | 5.273 | pass |
| radix | dense-native | 1024 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 3 | 1.107 | 1.145 | 3.662 | pass |
| radix | dense-native | 1024 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 3 | 0.716 | 0.802 | 2.271 | pass |
| radix | path-native | 1024 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 3 | 6.514 | 3.953 | 21.314 | pass |
| radix | path-native | 1024 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 3 | 5.150 | 3.222 | 19.485 | pass |
| radix | path-native | 1024 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 3 | 3.363 | 2.085 | 10.537 | pass |
| radix | singlepass-native | 1024 / 32 | g=4, leaf=0, Q=16, P=4, bits=32 | 3 | 1.609 | 1.536 | 7.818 | pass |
| radix | singlepass-native | 1024 / 32 | g=4, leaf=8, Q=16, P=4, bits=32 | 3 | 1.244 | 1.208 | 5.648 | pass |
| radix | singlepass-native | 1024 / 32 | g=4, leaf=16, Q=16, P=4, bits=32 | 3 | 0.913 | 0.927 | 3.513 | pass |
| posting | dense-native | 16384 / 32 | g=4, leaf=0, Q=128, P=4 | 3 | 0.314 | 0.203 | 2.149 | pass |
| posting | singlepass-native | 16384 / 32 | g=4, leaf=0, Q=128, P=4 | 3 | 0.175 | 0.193 | 3.683 | pass |
| posting | singlepass-native | 16384 / 32 | g=4, leaf=0, Q=128, P=16 | 3 | 0.380 | 0.249 | 3.791 | pass |
| posting | singlepass-native | 16384 / 32 | g=4, leaf=0, Q=128, P=32 | 3 | 0.679 | 0.437 | 4.173 | pass |
| posting | dense-native | 65536 / 32 | g=4, leaf=0, Q=128, P=4 | 3 | 2.427 | 0.431 | 10.553 | pass |
| posting | singlepass-native | 65536 / 32 | g=4, leaf=0, Q=128, P=4 | 3 | 0.199 | 0.229 | 14.763 | controller-transient-rss |
| posting | singlepass-native | 65536 / 32 | g=4, leaf=0, Q=128, P=16 | 3 | 0.436 | 0.314 | 14.661 | controller-transient-rss |
| posting | singlepass-native | 65536 / 32 | g=4, leaf=0, Q=128, P=32 | 3 | 0.725 | 0.419 | 15.184 | controller-transient-rss |
| posting | dense-native | 16384 / 32 | g=4, leaf=0, Q=4096, P=4 | 3 | 0.292 | 0.177 | 0.373 | pass |
| posting | singlepass-native | 16384 / 32 | g=4, leaf=0, Q=4096, P=4 | 3 | 0.167 | 0.173 | 0.280 | pass |
| posting | dense-native | 65536 / 32 | g=4, leaf=0, Q=4096, P=4 | 3 | 2.184 | 0.395 | 2.534 | pass |
| posting | singlepass-native | 65536 / 32 | g=4, leaf=0, Q=4096, P=4 | 3 | 0.205 | 0.218 | 0.625 | pass |
| radix | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 0.363 | 0.408 | 0.923 | pass |
| radix | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 0.364 | 0.442 | 0.935 | pass |
| radix | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 1.320 | 0.923 | 2.380 | pass |
| radix | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 1.328 | 0.919 | 2.422 | pass |
| hash | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 0.400 | 0.456 | 1.010 | pass |
| hash | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 0.405 | 0.446 | 1.036 | pass |
| hash | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 2.168 | 1.097 | 3.741 | pass |
| hash | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 2.169 | 1.117 | 3.759 | pass |
| bitmap | dense-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, value/8 | 3 | 4.700 | 11.563 | 14.260 | pass |
| bitmap | dense-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, key/8 | 3 | 4.735 | 11.696 | 14.298 | pass |
| bitmap | path-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, value/8 | 3 | 6.817 | 13.163 | 18.027 | pass |
| bitmap | path-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, key/8 | 3 | 6.907 | 13.290 | 18.307 | pass |
| wavelet | dense-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, value/8 | 3 | 3.683 | 4.164 | 4.943 | pass |
| wavelet | dense-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, key/8 | 3 | 3.651 | 4.108 | 4.915 | pass |
| wavelet | path-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, value/8 | 3 | 10.224 | 8.597 | 12.323 | pass |
| wavelet | path-native | 128 / 32 | g=16, leaf=0, Q=24, P=4, key/8 | 3 | 10.228 | 8.983 | 12.211 | pass |
| posting | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 0.120 | 0.166 | 0.700 | pass |
| posting | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 0.121 | 0.168 | 0.711 | pass |
| posting | singlepass-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 0.164 | 0.161 | 0.916 | pass |
| posting | singlepass-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 0.163 | 0.161 | 0.938 | pass |
| authenticated | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 2.387 | 2.567 | 3.539 | pass |
| authenticated | dense-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 2.378 | 2.483 | 3.719 | pass |
| authenticated | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, value/8 | 3 | 13.725 | 6.866 | 16.809 | pass |
| authenticated | path-native | 128 / 32 | g=4, leaf=0, Q=24, P=4, key/8 | 3 | 13.759 | 7.031 | 17.772 | pass |

Raw run paths, complete configuration columns, resource caps and per-run references are preserved in the adjacent CSV/JSON.
