// 02-ddl: Schema creation via drizzle-kit push.
// Instead of hand-writing CREATE TABLE, we let Drizzle generate the exact DDL
// it would produce for our schema.ts. This is what OpenCode does — drizzle-kit
// reads the schema definitions and emits its own DDL with its own quoting,
// column ordering, constraint syntax, and index naming.

import { sql } from "../db";
import { join } from "path";
import type { TestResult } from "../run";

const ROOT = new URL("../../", import.meta.url).pathname;

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Drop all tables first (reverse dependency order) — raw SQL cleanup
  results.push(
    await test("drop existing tables", async () => {
      await sql`DROP TABLE IF EXISTS session_share CASCADE`;
      await sql`DROP TABLE IF EXISTS permission CASCADE`;
      await sql`DROP TABLE IF EXISTS todo CASCADE`;
      await sql`DROP TABLE IF EXISTS part CASCADE`;
      await sql`DROP TABLE IF EXISTS message CASCADE`;
      await sql`DROP TABLE IF EXISTS session CASCADE`;
      await sql`DROP TABLE IF EXISTS project CASCADE`;
    })
  );

  // Run drizzle-kit push — Drizzle generates all DDL from schema.ts
  results.push(
    await test("drizzle-kit push (Drizzle-generated DDL)", async () => {
      const proc = Bun.spawn(
        ["bunx", "drizzle-kit", "push", "--force"],
        {
          cwd: ROOT,
          env: { ...process.env },
          stdout: "pipe",
          stderr: "pipe",
        }
      );
      const exitCode = await proc.exited;
      const stdout = await new Response(proc.stdout).text();
      const stderr = await new Response(proc.stderr).text();
      if (exitCode !== 0) {
        throw new Error(
          `drizzle-kit push failed (exit ${exitCode}):\n${stderr}\n${stdout}`
        );
      }
    })
  );

  // Verify all 7 tables were created
  results.push(
    await test("verify all tables via information_schema", async () => {
      const rows = await sql`
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
      `;
      const names = rows.map((r: any) => r.table_name).sort();
      const expected = [
        "message",
        "part",
        "permission",
        "project",
        "session",
        "session_share",
        "todo",
      ];
      for (const t of expected) {
        if (!names.includes(t))
          throw new Error(`Missing table: ${t}. Found: ${names.join(", ")}`);
      }
    })
  );

  // Verify indexes were created (Drizzle names them from the schema definition)
  results.push(
    await test("verify indexes via pg_indexes", async () => {
      const rows = await sql`
        SELECT indexname FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY indexname
      `;
      const names = rows.map((r: any) => r.indexname);
      const expected = [
        "session_project_idx",
        "session_parent_idx",
        "message_session_idx",
        "part_message_idx",
        "part_session_idx",
        "todo_session_idx",
      ];
      for (const idx of expected) {
        if (!names.includes(idx))
          throw new Error(`Missing index: ${idx}. Found: ${names.join(", ")}`);
      }
    })
  );

  // Verify foreign key constraints exist
  results.push(
    await test("verify foreign keys via information_schema", async () => {
      const rows = await sql`
        SELECT
          tc.table_name,
          tc.constraint_name,
          ccu.table_name AS foreign_table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
      `;
      const fks = rows.map(
        (r: any) => `${r.table_name} -> ${r.foreign_table_name}`
      );
      // Session references project
      if (!fks.some((f: string) => f.includes("session") && f.includes("project")))
        throw new Error("Missing FK: session -> project");
      // Message references session
      if (!fks.some((f: string) => f.includes("message") && f.includes("session")))
        throw new Error("Missing FK: message -> session");
      // Part references message
      if (!fks.some((f: string) => f.includes("part") && f.includes("message")))
        throw new Error("Missing FK: part -> message");
    })
  );

  // Verify composite primary key on todo
  results.push(
    await test("verify composite PK on todo", async () => {
      const rows = await sql`
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'todo'::regclass AND i.indisprimary
        ORDER BY a.attname
      `;
      const cols = rows.map((r: any) => r.attname).sort();
      if (!cols.includes("session_id") || !cols.includes("position"))
        throw new Error(`Expected composite PK (session_id, position), got: ${cols.join(", ")}`);
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
