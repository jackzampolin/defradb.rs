# ffi-test

CLI tool for running Go integration tests against the Rust FFI implementation of DefraDB. Validates behavioral compatibility by building the Rust FFI library, copying it into the Go test harness, and running Go's integration test packages.

## Install

```bash
cargo install --path tools/ffi-test
```

## Architecture

```
defradb.rs (Rust)                    defradb (Go)
├── crates/ffi/                      ├── tests/integration/
│   └── (FFI library)                │   └── (Go test packages)
├── tools/ffi-test/                  └── tests/clients/rustffi/
│   └── (this CLI tool)                  ├── libdefra_ffi.dylib
                                         └── defra.h
```

FFI test requires **paired worktrees** — a Rust worktree and a corresponding Go worktree side by side:

```
sourcenetwork/
├── defradb.rs      ←→  defradb       (main)
├── defradb.rs-foo  ←→  defradb-foo   (ffi/foo branch)
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
