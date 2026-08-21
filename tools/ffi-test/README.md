# ffi-test

CLI tool for running Go integration tests against the Rust FFI implementation of DefraDB. Validates behavioral compatibility by building the Rust FFI library into a Go checkout that already carries the Go-side test client, then running Go's integration test packages.

## Install

```bash
cargo install --path tools/ffi-test
```

## Architecture

```
defradb.rs (Rust)                     defradb (Go) @ the client branch
├── crates/ffi/                       ├── tests/integration/
│   └── (FFI library)                 │   └── (Go test packages)
└── tools/ffi-test/                   └── tests/clients/rustffi/
    └── (this CLI tool)      ──────▶      ├── (the Go client, on the branch)
                                          ├── defra.h        (generated)
                                          └── libdefra_ffi.* (generated)
```

The Go tree is a checkout of `sourcenetwork/defradb` on the branch that carries
the rustffi client — `jack/ffi-rust-compat`, or a branch based on it. That
branch is a maintained chore branch, not something upstream intends to merge;
it holds the client plus the harness seams the client needs.

`ffi-test` writes exactly two things into that checkout, both regenerated from
`crates/ffi` on every run and neither ever committed:

- **`defra.h`** — cbindgen output.
- **the shared library** — `cargo build --release -p ffi`, copied in as
  `libdefra_ffi.*`.

Everything else in the Go tree belongs to the Go side. Changes to the client
itself are commits on that branch, not edits here.

## Resolving the Go checkout

In order of precedence:

1. `--go-path <PATH>`
2. `DEFRADB_GO_REPO`
3. **worktree pairing** — a Rust worktree and a Go worktree side by side,
   sharing a suffix:

```text
sourcenetwork/
├── defradb.rs      ←→  defradb       (main)
├── defradb.rs-foo  ←→  defradb-foo   (feature branch)
```

## Commands

### Running Tests

| Command | Purpose |
|---------|---------|
| `ffi-test run query/simple` | Run specific package |
| `ffi-test run query` | Auto-split: discovers and runs all query/* subpackages separately |
| `ffi-test run query/simple -t TestName` | Run specific test |
| `ffi-test run query/simple -v` | Verbose output (streams to terminal) |
| `ffi-test run query/simple --skip-build` | Skip FFI rebuild |

### Status

| Command | Purpose |
|---------|---------|
| `ffi-test status` | Show pass rates at top-level (query, mutation, net) |
| `ffi-test status -d 2` | Show second level (query/simple, query/commits) |
| `ffi-test status -d 3` | Show full detail (query/simple/with_filter) |
| `ffi-test status --all` | Show all worktrees summary |

### Logs

| Command | Purpose |
|---------|---------|
| `ffi-test logs query/simple` | Show logs from last run (failed tests only) |
| `ffi-test logs query/simple --failed` | Show only failed test output |
| `ffi-test logs query/simple -t TestName` | Show logs for specific test |
| `ffi-test logs query/simple --all` | Show output for all tests |
| `ffi-test diff query/simple` | Compare last two runs |

### Worktree Management

| Command | Purpose |
|---------|---------|
| `ffi-test worktree create foo` | Create paired Rust+Go worktrees |
| `ffi-test worktree list` | List all pairs |
| `ffi-test worktree remove foo` | Remove both worktrees |

## Auto-Split Package Runs

When you run a parent package like `ffi-test run query`, the tool automatically:

1. **Discovers all subpackages** recursively (query/simple, query/simple/with_filter, etc.)
2. **Runs each separately** (non-recursive, just that package's tests)
3. **Shows progress** as each package completes:
   ```
   Running tests: query (17 packages found)

   Package                                              Pass     Fail     Skip     Rate
   ────────────────────────────────────────────────────────────────────────────────────
   ✓ query/commits                                        12        0        3     100%
   ✗ query/inline_array                                   15        2        0      88%
   ...
   ────────────────────────────────────────────────────────────────────────────────────
     TOTAL (17 packages)                                 435       12       42      97%
   ```
4. **Saves separate reports** for each package (enables per-package status and logs)
5. **Lists all failures** grouped by package at the end

## Debugging Workflow

```bash
# Run tests for your package
ffi-test run query/simple

# See what failed
ffi-test logs query/simple --failed

# Dig into specific test
ffi-test logs query/simple -t TestQueryWithIndex --all

# Add debug logging to Rust code, rebuild, run again
ffi-test run query/simple

# Check your debug output
ffi-test logs query/simple -t TestQueryWithIndex --all
```

## Status Colors

| Color | Pass Rate | Meaning |
|-------|-----------|---------|
| Green | 100% | Complete |
| Yellow | 90-99% | Almost there |
| Red | <90% | Needs work |

When running from the **main branch**, status shows a unified view of the latest results from ALL worktrees. On feature branches, status shows only results from that branch.

## Reports

Reports are saved to `~/.defra-ffi-reports/runs/` as JSON files with full test output.
