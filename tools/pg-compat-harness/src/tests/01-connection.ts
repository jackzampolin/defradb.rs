// 01-connection: Can Drizzle connect and run introspection queries?
// These are the exact queries Drizzle issues on startup.

import { sql } from "../db";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Drizzle startup: SELECT current_schema()
  results.push(
    await test("current_schema()", async () => {
      const rows = await sql`SELECT current_schema()`;
      if (!rows[0]?.current_schema) throw new Error("No current_schema returned");
    })
  );

  // Drizzle startup: information_schema.tables
  results.push(
    await test("information_schema.tables", async () => {
      const rows = await sql`
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
      `;
      // Just needs to not error — may be empty before DDL
      if (!Array.isArray(rows)) throw new Error("Expected array result");
    })
  );

  // Drizzle startup: information_schema.columns
  results.push(
    await test("information_schema.columns", async () => {
      const rows = await sql`
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
        LIMIT 1
      `;
      if (!Array.isArray(rows)) throw new Error("Expected array result");
    })
  );

  // Drizzle startup: pg_catalog type query
  results.push(
    await test("pg_catalog.pg_type", async () => {
      const rows = await sql`
        SELECT oid, typname FROM pg_catalog.pg_type LIMIT 5
      `;
      if (rows.length === 0) throw new Error("No types returned");
    })
  );

  // Basic prepared statement (postgres.js uses extended query protocol)
  results.push(
    await test("parameterized query", async () => {
      const val = "hello";
      const rows = await sql`SELECT ${val}::text AS greeting`;
      if (rows[0]?.greeting !== "hello") throw new Error("Param binding failed");
    })
  );

  return results;
}

async function test(
  name: string,
  fn: () => Promise<void>
): Promise<TestResult> {
  try {
    await fn();
    return { name, passed: true };
  } catch (e: any) {
    return { name, passed: false, error: e.message };
  }
}
