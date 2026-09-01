# MPC4J Practical Keyword PIR artifact gate

This directory records an independent gate of the artifact for *Practical
Keyword Private Information Retrieval from Key-to-Index Mappings* (USENIX
Security 2025). It is not a Rust implementation and it is not linked into
DefraDB.

## Provenance

- Upstream: <https://github.com/alibaba-edu/mpc4j>
- Release tag: `v1.1.3-beta`
- Git commit: `178da1b07e8aa011bce9ffbd921e8a1d24477f0b`
- Zenodo record: <https://zenodo.org/records/14722434>
- Zenodo DOI: `10.5281/zenodo.14722434`
- Zenodo archive metadata: 36,378,721 bytes, MD5
  `dc27980fc4c3fa01d43c6767650184cf`
- Paper: <https://www.usenix.org/conference/usenixsecurity25/presentation/hao>
- Official artifact instructions:
  `ae/2025_USEC_Practical_Keyword_PIR_from_Key-to-Index_Mappings.md` in the
  pinned checkout.

The checkout lives only in ignored
`target/pir-research/mpc4j-1.1.3-beta`; no upstream source is vendored. The
requested name `SIMPLE_NATIVE` does not exist at this revision. The upstream
artifact calls KPIR^kvs `SIMPLE_NAIVE` and its classes start with
`SimpleNaive`. The other exact names are `SIMPLE_BIN` (KPIR^hash),
`PGM_INDEX` (KPIR^index), and `CHALAMET`.

## Environment and build

The build is the smallest Maven reactor closure selected by
`-pl mpc4j-s2pc-pir -am`. The four selected protocols are pure Java, so no
MPC4J native FHE/JNI library is built or loaded. The closure still compiles
the Java `mpc4j-crypto-fhe` module because it is an unconditional dependency
of `mpc4j-s2pc-pir`.

```bash
git clone --branch v1.1.3-beta --depth 1 \
  https://github.com/alibaba-edu/mpc4j.git \
  target/pir-research/mpc4j-1.1.3-beta
cd target/pir-research/mpc4j-1.1.3-beta
mvn -pl mpc4j-s2pc-pir -am -DskipTests package
```

For the target-only Maven test commands below, install the built reactor jars
in the local Maven cache first (or add `-am` plus
`-Dsurefire.failIfNoSpecifiedTests=false` to every gate):

```bash
mvn -pl mpc4j-s2pc-pir -am -DskipTests install
```

Runner dependencies are OpenJDK 17.0.19, Maven 3.9.12, and Ubuntu under
WSL2. This gate saw 16 logical CPUs and 8,097,436 KiB total WSL memory. The
host is recorded in `../ARTIFACTS.md`. The cold package build passed in
34:02.77 wall time with 1,185,672 KiB peak RSS. Most of that wall time was
Maven I/O on the Windows-mounted workspace; it is not protocol setup time.

## Correctness gates

The tests are run before the performance harness and without changing the
pinned source. `SimpleCpKsPirParamsTest` is intentionally annotated
`@Ignore` upstream; the skipped outcome is evidence about the official
artifact, not a pass. Parameter geometry for the local 96-byte point is
therefore derived from the exact pinned descriptor methods and cross-checked
with the compiled Java classes.

The untouched gates are:

```bash
mvn -pl mpc4j-common-structure \
  -Dmaven.test.skip=false -Dtest=LongApproxPgmIndexTest test
mvn -pl mpc4j-s2pc-pir \
  -Dmaven.test.skip=false -Dtest=SimpleCpKsPirParamsTest test
mvn -pl mpc4j-s2pc-pir \
  -Dmaven.test.skip=false -Dtest='CpKsPirTest#testDefault*' test
```

The explicit `maven.test.skip=false` is essential: the pinned root POM sets
`maven.test.skip=true`, so a plain Maven `test` command returns success while
compiling and executing zero tests. The wildcard is also essential because
JUnit's parameterized runner appends the configuration name to
`testDefault`.

`CpKsPirTest` contains present and absent keywords, verifies exact returned
values or `null`, and runs both individual and batch API paths. It is
parameterized over nine client-preprocessing configurations; the gate
therefore includes the requested four plus five configurations outside this
artifact scope.

### Observed gate results

| Gate | Result | Wall / peak RSS |
|---|---|---|
| `LongApproxPgmIndexTest` | 4/4 pass, including the one-million-key range test and serialize/deserialize range checks | 1:30.46 including first test compile; 627,628 KiB |
| `SimpleCpKsPirParamsTest` | 1 discovered, 1 skipped because the entire class is `@Ignore` upstream | 1:05.63 including first PIR test compile; 383,520 KiB |
| `CpKsPirTest#testDefault*` | Requested four pass; suite reports 9 run / 5 errors from unrelated native-dependent configs | 1:07.41; 371,960 KiB |

The requested cases and their Surefire durations were:

| Requested configuration | Result | Surefire duration |
|---|---:|---:|
| `PGM_INDEX` | pass | 2.295 s |
| `SIMPLE_BIN` | pass | 0.938 s |
| `SIMPLE_NAIVE` | pass | 1.189 s |
| `CHALAMET` | pass | 6.267 s |

Each case invokes the official `testPto` twice: once through individual calls
and once through the batch API, at 4,093 keys, 8-byte values, and two queries
(one present, one absent). The five errors are `PAI_CKS` plus four `ALPR21`
variants trying to load absent `mpc4j-native-tool`; there were no assertion
failures. This suite-level failure must not be reported as a failure of the
four pure-Java constructions, nor hidden as an overall pass.

## Local corpus point and measurement definitions

`defra-n18-l768.conf` uses the Defra comparison dimensions exactly:

- 262,144 keyword/value pairs (`server_log_set_size = 18`)
- 96-byte values (`entry_bit_length = 768`)
- 100 sequential queries
- one server and one client on loopback
- protocol parallelism disabled

The dimensions and useful value width are identical, but the contents are
not. MPC4J's official main program generates random values and uses decimal
ordinal strings as keywords; it has no adapter for the Defra encoded page
file. Results here are consequently `exact-dimensions / synthetic-contents`,
not a byte-for-byte Defra corpus run.

Run one protocol with:

```bash
tools/pir-poc/research/kpir_artifact/run-local.sh PGM_INDEX
```

Raw logs and outputs stay ignored under
`target/pir-research/kpir-artifact-results/`. Measurements mean:

- **server/client init**: each party's wall time around `init` in the official
  main program; it includes protocol construction and hint transfer/waiting;
- **server answer**: server wall time for 100 `pir()` calls divided by 100;
- **client PIR**: client wall time for query, wait, and recovery divided by
  100; this is end-to-end client-observed latency, not pure client CPU;
- **upload/download**: official RPC `Send Bytes`, divided by 100, rather than
  payload-only bytes; payload values are retained separately;
- **process peak RSS**: GNU `time -v` peak for the complete Java process,
  including the JVM, input arrays/maps, protocol state, and transient setup;
- **persistent primitive state**: exact `int`/byte matrix payload implied by
  the pinned implementation. It is a lower bound that excludes Java object
  headers, maps, RPC state, and the JVM;
- **layout expansion**: encoded protocol cells divided by the 25,165,824
  useful value bytes. A separate factor reports the four-byte Java `int`
  representation where applicable.

Paper results and local results are kept in separate tables. No paper number
is substituted for a failed, timed-out, or memory-blocked local run.

### Exact-source analytical record at 262,144 x 96 bytes

The clean two-process performance sweep is deliberately separate from this
correctness gate. The following values are computed from the pinned
descriptor/source formulas and checked with the compiled Java classes; they
are not measured query timings.

| Protocol | Layout geometry | Server primitive state | Client primitive state | Query payload | Response payload | Encoded layout / useful values |
|---|---|---:|---:|---:|---:|---:|
| `SIMPLE_NAIVE` | 303,104-slot 3-XOR fuse; SimplePIR 5,616 x 5,614 x 1 | 126,112,896 B | >=46,132,840 B | 67,368 B | 67,392 B | 1.2526x |
| `SIMPLE_BIN` | 5,222 bins x estimated max 89 x 104 | 193,339,328 B estimated | >=59,359,800 B estimated | 20,888 B | 37,024 B estimated | 1.9207x estimated |
| `PGM_INDEX` | 62 x 5,222 x 104 (51 data rows plus 11 overlap rows) | >=134,685,824 B plus PGM index | >=47,847,000 B | 20,888 B | 25,792 B | 1.3380x |
| `CHALAMET` | 303,104-slot 3-XOR fuse x 104 | 126,091,264 B | >=1,950,816 B | 1,212,416 B | 416 B | 1.2526x |

“Primitive state” counts retained `int` vectors/matrices and directly
retained byte payloads only. It excludes object headers, maps, RPC objects,
the JVM, and transient setup matrices. `SIMPLE_BIN` uses the observed maximum
bucket size at initialization; 89 is the artifact's 40-bit-security estimate,
so only an actual seeded build can replace those estimated cells and response
bytes. All online payload numbers exclude RPC framing.

The useful output is 96 bytes. Chalamet's small response is paired with a
1.16 MiB query upload and an expensive client key-preprocessing pass over its
303,104-slot matrix. The three proposed KPIR mappings move that trade-off:
larger responses and client hints, but much smaller upload.

### Paper-only context

The authors report 15--178x lower communication and 1.1--2.4x lower runtime
than their Chalamet baseline across paper configurations, and about 47 ms for
a one-million-entry, 32-byte query point. Those are author measurements on a
different machine/corpus/width and do not appear in a local-result column.
The configured 262,144 x 96-byte sweep remains pending a clean shared runner
window; `run-local.sh` is the reproducible entry point.

## PGM correctness and production qualifications

`PGM_INDEX` hashes each keyword to a 64-bit integer, sorts those integers,
and builds an approximate PGM rank model with epsilon 4. The client uses the
predicted rank to select one matrix column. Each column embeds overlap rows
from its neighbours, so a bounded rank error still includes the record. The
client scans the returned rows and accepts only an entry whose 8-byte digest
matches the requested keyword.

This is not exact MPHF semantics:

- `LongApproxPgmIndex` documents that the expected range contains the true
  rank with very high probability, but that rare Java `double` precision
  errors can escape the range. The protocol can then return a false miss.
- The server stores the 64-bit index hashes in a `Map<Long,T>` with ordinary
  replacement semantics. A collision can drop one keyword. At 262,144
  uniformly hashed keys the birthday collision probability is about
  1.86e-9 per build.
- The 8-byte digest provides probabilistic membership rejection (nominal
  random false acceptance 2^-64); it is not a MAC, signature, freshness
  proof, or verifiable-PIR proof. All four configs declare the semi-honest
  security model.

For production, use a wider collision-resolving key-to-ordinal layer, verify
all mappings while building, and authenticate/sign returned Defra documents
independently. A PGM miss should not silently prove absence.

## Updates

All four implementations initialize from a complete map and expose no server
insert/delete/update method. An update changes a fuse filter, bin occupancy,
or sorted PGM ranks and also invalidates the LWE hint. It therefore requires
rebuilding protocol state and refreshing the client hint. The similarly named
client `updateKeys()` only rotates/precomputes LWE client secrets; it does not
update the database.

The viable Defra integration is immutable, versioned epochs: build the next
epoch off-path, publish its authenticated identifier and hint, atomically
switch readers, and retain old epochs for historical queries. A small mutable
delta needs a separate PIR layer and periodic compaction; it is not supplied
by this artifact.
