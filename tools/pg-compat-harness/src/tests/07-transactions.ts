// 07-transactions: BEGIN/COMMIT/ROLLBACK behavior.

import { db, sql } from "../db";
import { eq } from "drizzle-orm";
import { ProjectTable, SessionTable, MessageTable } from "../schema";
import { makeProject, makeSession, makeMessage } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Transaction that commits
  results.push(
    await test("transaction commit", async () => {
      const proj = makeProject({ name: "tx-commit-test" });
      const sess = makeSession(proj.id);

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
      });

      // Verify both exist outside transaction
      const projects = await db
        .select()
        .from(ProjectTable)
        .where(eq(ProjectTable.id, proj.id));
      if (projects.length !== 1) throw new Error("Project not committed");

      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (sessions.length !== 1) throw new Error("Session not committed");

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Transaction that rolls back on error
  results.push(
    await test("transaction rollback on error", async () => {
      const proj = makeProject({ name: "tx-rollback-test" });
      const sess = makeSession(proj.id);

      try {
        await db.transaction(async (tx) => {
          await tx.insert(ProjectTable).values(proj);
          // This should fail — session references a valid project, but let's
          // force a rollback by throwing
          throw new Error("deliberate rollback");
        });
      } catch (e: any) {
        if (!e.message.includes("deliberate rollback")) throw e;
      }

      // Project should NOT exist (rolled back)
      const projects = await db
        .select()
        .from(ProjectTable)
        .where(eq(ProjectTable.id, proj.id));
      if (projects.length !== 0)
        throw new Error("Project should have been rolled back");
    })
  );

  // Transaction with multiple operations
  results.push(
    await test("transaction multi-operation", async () => {
      const proj = makeProject({ name: "tx-multi-test" });
      const sess = makeSession(proj.id);
      const msg1 = makeMessage(sess.id, "user");
      const msg2 = makeMessage(sess.id, "assistant");

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx.insert(MessageTable).values([msg1, msg2]);
        // Update within same transaction
        await tx
          .update(SessionTable)
          .set({ title: "Updated in TX" })
          .where(eq(SessionTable.id, sess.id));
      });

      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (sessions[0]?.title !== "Updated in TX")
        throw new Error("Title not updated in transaction");

      const messages = await db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.session_id, sess.id));
      if (messages.length !== 2) throw new Error(`Expected 2 messages, got ${messages.length}`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Raw SQL transaction (postgres.js begin/end)
  results.push(
    await test("raw SQL transaction", async () => {
      const proj = makeProject({ name: "tx-raw-test" });
      await sql.begin(async (tx) => {
        await tx`INSERT INTO project (id, worktree, name, time_created, time_updated, sandboxes) VALUES (${proj.id}, ${proj.worktree}, ${proj.name}, ${proj.time_created}, ${proj.time_updated}, ${proj.sandboxes})`;
      });
      const check = await sql`SELECT id FROM project WHERE id = ${proj.id}`;
      if (check.length !== 1) throw new Error("Raw TX insert failed");

      // Cleanup
      await sql`DELETE FROM project WHERE id = ${proj.id}`;
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
