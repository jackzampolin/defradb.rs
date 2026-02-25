// Spin up an embedded Postgres instance in .pg-data/, run the full test suite, tear down.
// Usage: bun run src/embedded-test.ts
//
// Requires: initdb and postgres binaries on PATH (from `brew install postgresql@16`)
// Data dir is gitignored at .pg-data/

import { $ } from "bun";
import { existsSync, mkdirSync, rmSync } from "fs";
import { join } from "path";

const ROOT = new URL("../", import.meta.url).pathname;
const DATA_DIR = join(ROOT, ".pg-data");
const LOG_FILE = join(ROOT, ".pg-data", "postgres.log");
const SOCKET_DIR = "/tmp";
const PORT = 15432; // non-standard port to avoid conflicts
const DB_NAME = "pgcompat";
const PG_URL = `postgres://127.0.0.1:${PORT}/${DB_NAME}`;

async function findBinary(name: string): Promise<string> {
  // Check common Homebrew paths first
  const brewPaths = [
    `/opt/homebrew/opt/postgresql@17/bin/${name}`,
    `/opt/homebrew/opt/postgresql@16/bin/${name}`,
    `/opt/homebrew/opt/postgresql@15/bin/${name}`,
    `/opt/homebrew/bin/${name}`,
    `/usr/local/opt/postgresql@17/bin/${name}`,
    `/usr/local/opt/postgresql@16/bin/${name}`,
    `/usr/local/bin/${name}`,
  ];
  for (const p of brewPaths) {
    if (existsSync(p)) return p;
  }
  // Fall back to PATH
  try {
    const result = await $`which ${name}`.text();
    return result.trim();
  } catch {
    throw new Error(
      `${name} not found. Install PostgreSQL: brew install postgresql@16`
    );
  }
}

async function main() {
  const initdbBin = await findBinary("initdb");
  const pgBin = await findBinary("postgres");
  const createdbBin = await findBinary("createdb");

  console.log(`initdb:   ${initdbBin}`);
  console.log(`postgres: ${pgBin}`);
  console.log(`createdb: ${createdbBin}`);
  console.log(`data dir: ${DATA_DIR}`);
  console.log(`port:     ${PORT}`);

  // Clean up any previous data dir
  if (existsSync(DATA_DIR)) {
    console.log("Removing previous data dir...");
    rmSync(DATA_DIR, { recursive: true, force: true });
  }
  mkdirSync(DATA_DIR, { recursive: true });

  // Initialize the database cluster
  console.log("\nInitializing database cluster...");
  const initResult = await $`${initdbBin} -D ${DATA_DIR} --no-locale --encoding=UTF8 --auth=trust`
    .env({ ...process.env })
    .nothrow()
    .quiet();
  if (initResult.exitCode !== 0) {
    console.error("initdb failed:", initResult.stderr.toString());
    process.exit(1);
  }

  // Start postgres
  console.log("Starting PostgreSQL...");
  const pgProc = Bun.spawn(
    [
      pgBin,
      "-D", DATA_DIR,
      "-p", String(PORT),
      "-k", SOCKET_DIR,
      "-c", "listen_addresses=127.0.0.1",
      "-c", "log_min_messages=warning",
      "-c", "shared_buffers=128MB",
      "-c", "fsync=off",         // speed: test-only, no durability needed
      "-c", "synchronous_commit=off",
      "-c", "full_page_writes=off",
    ],
    {
      stdout: Bun.file(LOG_FILE),
      stderr: Bun.file(LOG_FILE),
    }
  );

  // Wait for postgres to be ready (nothrow() prevents Bun from throwing on non-zero exit)
  console.log("Waiting for PostgreSQL to start...");
  const pgIsReadyBin = await findBinary("pg_isready");
  let ready = false;
  for (let i = 0; i < 50; i++) {
    const check = await $`${pgIsReadyBin} -h 127.0.0.1 -p ${PORT}`.nothrow().quiet();
    if (check.exitCode === 0) {
      ready = true;
      break;
    }
    await Bun.sleep(200);
  }

  if (!ready) {
    console.error("PostgreSQL failed to start. Log contents:");
    try {
      const log = await Bun.file(LOG_FILE).text();
      console.error(log);
    } catch {}
    pgProc.kill();
    process.exit(1);
  }
  console.log("PostgreSQL is ready.");

  // Create test database
  console.log(`Creating database '${DB_NAME}'...`);
  await $`${createdbBin} -h 127.0.0.1 -p ${PORT} ${DB_NAME}`.quiet();

  // Run the test suite
  console.log(`\nRunning tests against ${PG_URL}\n`);
  let exitCode = 0;
  try {
    const testProc = Bun.spawn(["bun", "run", join(ROOT, "src", "run.ts")], {
      cwd: ROOT,
      env: { ...process.env, PG_URL },
      stdout: "inherit",
      stderr: "inherit",
    });
    exitCode = await testProc.exited;
  } catch (e: any) {
    console.error("Test runner failed:", e.message);
    exitCode = 2;
  }

  // Tear down
  console.log("\nStopping PostgreSQL...");
  pgProc.kill("SIGTERM");
  // Give it a moment to shut down cleanly
  await Bun.sleep(500);
  try {
    pgProc.kill("SIGKILL");
  } catch {}

  console.log("Cleaning up data dir...");
  rmSync(DATA_DIR, { recursive: true, force: true });

  console.log("Done.");
  process.exit(exitCode);
}

main().catch((e) => {
  console.error("Fatal:", e);
  // Try cleanup
  try {
    rmSync(DATA_DIR, { recursive: true, force: true });
  } catch {}
  process.exit(2);
});
