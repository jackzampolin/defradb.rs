// 04-select: All SELECT patterns OpenCode generates via Drizzle.
// eq, and, inArray, like, orderBy, limit, column subsets.

import { db } from "../db";
import { eq, and, inArray, like, desc, asc } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  PartTable,
  TodoTable,
} from "../schema";
import { makeProject, makeSession, makeMessage, makeTextPart } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed data: 1 project, 3 sessions, messages + parts
  const proj = makeProject({ name: "select-test-project" });
  await db.insert(ProjectTable).values(proj);

  const sessions = [
    makeSession(proj.id, { title: "Alpha session", time_updated: 1000 }),
    makeSession(proj.id, { title: "Beta session", time_updated: 2000 }),
    makeSession(proj.id, {
      title: "Gamma search target",
      time_updated: 3000,
      parent_id: null,
    }),
  ];
  // Set parent_id on third session to first session for AND filter test
  sessions[2] = { ...sessions[2], parent_id: sessions[0].id };
  await db.insert(SessionTable).values(sessions);

  const msgs = sessions.map((s) => makeMessage(s.id, "user"));
  await db.insert(MessageTable).values(msgs);

  const parts = msgs.map((m, i) =>
    makeTextPart(m.id, sessions[i].id, `Content for session ${i}`)
  );
  await db.insert(PartTable).values(parts);

  // SELECT by PK (LIMIT 1)
  results.push(
    await test("select by PK", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sessions[0].id))
        .limit(1);
      if (rows.length !== 1) throw new Error("Expected 1 row");
      if (rows[0].title !== "Alpha session") throw new Error("Wrong title");
    })
  );

  // SELECT with AND filter
  results.push(
    await test("select with AND", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            eq(SessionTable.parent_id, sessions[0].id)
          )
        );
      if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length}`);
      if (rows[0].title !== "Gamma search target") throw new Error("Wrong row");
    })
  );

  // SELECT with IN
  results.push(
    await test("select with IN", async () => {
      const ids = [sessions[0].id, sessions[2].id];
      const rows = await db
        .select()
        .from(SessionTable)
        .where(inArray(SessionTable.id, ids));
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // SELECT with LIKE
  results.push(
    await test("select with LIKE", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(like(SessionTable.title, "%search target%"));
      if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length}`);
    })
  );

  // ORDER BY DESC
  results.push(
    await test("select ORDER BY DESC", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id));
      if (rows[0].title !== "Gamma search target")
        throw new Error("Wrong order — expected Gamma first");
    })
  );

  // ORDER BY ASC
  results.push(
    await test("select ORDER BY ASC", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(asc(SessionTable.time_updated));
      if (rows[0].title !== "Alpha session")
        throw new Error("Wrong order — expected Alpha first");
    })
  );

  // LIMIT
  results.push(
    await test("select with LIMIT", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.time_updated))
        .limit(2);
      if (rows.length !== 2) throw new Error(`Expected 2, got ${rows.length}`);
    })
  );

  // LIMIT + OFFSET
  results.push(
    await test("select with LIMIT + OFFSET", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.time_updated))
        .limit(1)
        .offset(1);
      if (rows.length !== 1) throw new Error(`Expected 1, got ${rows.length}`);
      if (rows[0].title !== "Beta session")
        throw new Error("Wrong row after offset");
    })
  );

  // Column subset select
  results.push(
    await test("select column subset", async () => {
      const rows = await db
        .select({
          id: ProjectTable.id,
          name: ProjectTable.name,
          worktree: ProjectTable.worktree,
        })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, proj.id));
      if (rows.length !== 1) throw new Error("Expected 1 row");
      const row = rows[0] as Record<string, unknown>;
      if (row.id !== proj.id) throw new Error("Wrong id");
      // Should NOT have other columns in the result type
      if ("sandboxes" in row) throw new Error("Got unrequested column");
    })
  );

  // Multi-column ORDER BY (parts ordered by message_id, then id)
  results.push(
    await test("multi-column ORDER BY", async () => {
      const rows = await db
        .select()
        .from(PartTable)
        .orderBy(asc(PartTable.message_id), asc(PartTable.id));
      if (rows.length < 3) throw new Error(`Expected >=3 parts, got ${rows.length}`);
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
