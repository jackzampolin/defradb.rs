# pg-compat-harness

Drizzle ORM compatibility test harness for DefraDB's Postgres wire protocol (`defradb.rs/crates/pg-compat/`).

Exercises the exact query patterns that OpenCode generates via Drizzle ORM. The PG wire implementation is "done" when all 10 test categories pass against DefraDB the same way they pass against real Postgres.

## Prerequisites

- [Bun](https://bun.sh/) runtime
- PostgreSQL binaries on PATH (`brew install postgresql@16`)

## Quick Start

```bash
# Install dependencies
bun install

# Run against embedded Postgres (spins up, tests, tears down automatically)
bun test

# Run against an external Postgres
PG_URL=postgres://postgres:test@localhost:5432/postgres bun run test:external

# Run against DefraDB PG wire endpoint
PG_URL=postgres://localhost:5433/defradb bun run test:external
```

## Test Categories

| # | Category | What it tests |
|---|----------|--------------|
| 01 | connection | Drizzle startup queries: `current_schema()`, `information_schema`, `pg_catalog`, parameterized queries |
| 02 | ddl | `CREATE TABLE`, `CREATE INDEX`, foreign keys, composite primary keys |
| 03 | insert | Basic insert, `INSERT ... RETURNING`, upsert (`ON CONFLICT DO UPDATE`), multi-row insert |
| 04 | select | `eq`, `and`, `inArray`, `like`, `ORDER BY`, `LIMIT`/`OFFSET`, column subsets |
| 05 | update | Single/multi-field `SET`, `SET NULL`, `UPDATE ... RETURNING` |
| 06 | delete | Single/AND conditions, `CASCADE` verification |
| 07 | transactions | `BEGIN`/`COMMIT`, rollback on error, multi-operation transactions |
| 08 | pagination | `LIMIT`/`OFFSET` loop over 120 rows, order consistency across pages |
| 09 | json | JSON-as-text columns: read/write complex objects (messages, tool invocations, diffs) |
| 10 | session-lifecycle | Full OpenCode flow: create project → session → messages → parts → fork → share → delete cascade |

## Schema

Exact replica of OpenCode's 7 tables translated from SQLite to PG types:

- **project** — workspace/repo context
- **session** — conversation session with summary metadata
- **message** — messages within a session (JSON data column)
- **part** — message parts: text, tool invocations (JSON data column)
- **todo** — session task list (composite PK: session_id + position)
- **permission** — project-level permission rulesets (JSON data column)
- **session_share** — shared session links

## How It Works

1. `embedded-test.ts` runs `initdb` + `postgres` in `.pg-data/` (gitignored) on port 15432
2. Creates a `pgcompat` database
3. Spawns `run.ts` with `PG_URL` pointing at the embedded instance
4. `run.ts` executes test categories 01-10 in order
5. Results are printed to stdout and optionally written to `results/` as JSON
6. Embedded Postgres is stopped and `.pg-data/` is cleaned up

## Tracking Progress

When developing the DefraDB PG wire protocol, run against DefraDB and note which categories pass:

```
"We pass through 04-select, failing on 05-update RETURNING"
```

This gives a clear milestone for wire protocol implementation progress.
