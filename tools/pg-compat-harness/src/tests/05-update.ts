// 05-update: UPDATE patterns — single field, multi-field, SET NULL, RETURNING.

import { db } from "../db";
import { eq } from "drizzle-orm";
import { ProjectTable, SessionTable, TodoTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed
  const proj = makeProject({ name: "update-test" });
  await db.insert(ProjectTable).values(proj);
  const sess = makeSession(proj.id, { share_url: "https://old.example.com" });
  await db.insert(SessionTable).values(sess);
  const todo = makeTodo(sess.id, 0, { status: "pending" });
  await db.insert(TodoTable).values(todo);

  // Single field update
  results.push(
    await test("update single field", async () => {
      const newTime = Date.now();
      await db
        .update(SessionTable)
        .set({ time_updated: newTime })
        .where(eq(SessionTable.id, sess.id));
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0].time_updated !== newTime)
        throw new Error("time_updated not changed");
    })
  );

  // Multi-field update
  results.push(
    await test("update multiple fields", async () => {
      await db
        .update(SessionTable)
        .set({
          summary_additions: 42,
          summary_deletions: 7,
          summary_files: 3,
          revert: JSON.stringify({ messageID: "msg_123" }),
        })
        .where(eq(SessionTable.id, sess.id));
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0].summary_additions !== 42) throw new Error("additions wrong");
      if (rows[0].summary_deletions !== 7) throw new Error("deletions wrong");
      if (rows[0].summary_files !== 3) throw new Error("files wrong");
    })
  );

  // SET to NULL
  results.push(
    await test("update SET NULL", async () => {
      await db
        .update(SessionTable)
        .set({ share_url: null })
        .where(eq(SessionTable.id, sess.id));
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0].share_url !== null) throw new Error("share_url not null");
    })
  );

  // UPDATE ... RETURNING
  results.push(
    await test("update with RETURNING", async () => {
      const rows = await db
        .update(SessionTable)
        .set({ title: "Updated Title" })
        .where(eq(SessionTable.id, sess.id))
        .returning();
      if (rows.length !== 1) throw new Error(`Expected 1, got ${rows.length}`);
      if (rows[0].title !== "Updated Title") throw new Error("Wrong title");
      if (rows[0].id !== sess.id) throw new Error("Wrong id in RETURNING");
    })
  );

  // Update on composite PK table
  results.push(
    await test("update composite PK table", async () => {
      await db
        .update(TodoTable)
        .set({ status: "completed", content: "Done!" })
        .where(eq(TodoTable.session_id, sess.id));
      const rows = await db
        .select()
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id));
      if (rows[0].status !== "completed") throw new Error("Status not updated");
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
