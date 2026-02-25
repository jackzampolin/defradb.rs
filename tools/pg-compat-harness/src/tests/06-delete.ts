// 06-delete: DELETE with conditions, CASCADE verification.

import { db } from "../db";
import { eq, and } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  PartTable,
  TodoTable,
  SessionShareTable,
} from "../schema";
import {
  makeProject,
  makeSession,
  makeMessage,
  makeTextPart,
  makeTodo,
  makeSessionShare,
} from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed a full object graph: project → session → message → part, todo, share
  const proj = makeProject({ name: "delete-test" });
  await db.insert(ProjectTable).values(proj);
  const sess = makeSession(proj.id);
  await db.insert(SessionTable).values(sess);
  const msg = makeMessage(sess.id, "user");
  await db.insert(MessageTable).values(msg);
  const part1 = makeTextPart(msg.id, sess.id, "part 1");
  const part2 = makeTextPart(msg.id, sess.id, "part 2");
  await db.insert(PartTable).values([part1, part2]);
  await db.insert(TodoTable).values(makeTodo(sess.id, 0));
  await db.insert(SessionShareTable).values(makeSessionShare(sess.id));

  // Delete with single condition
  results.push(
    await test("delete single condition", async () => {
      await db.delete(TodoTable).where(eq(TodoTable.session_id, sess.id));
      const rows = await db
        .select()
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id));
      if (rows.length !== 0) throw new Error("Todo not deleted");
    })
  );

  // Delete with AND condition (OpenCode pattern for parts)
  results.push(
    await test("delete with AND", async () => {
      await db
        .delete(PartTable)
        .where(
          and(eq(PartTable.id, part1.id), eq(PartTable.session_id, sess.id))
        );
      const rows = await db
        .select()
        .from(PartTable)
        .where(eq(PartTable.session_id, sess.id));
      if (rows.length !== 1) throw new Error(`Expected 1 part left, got ${rows.length}`);
      if (rows[0].id !== part2.id) throw new Error("Wrong part deleted");
    })
  );

  // CASCADE: delete session → should cascade to messages, parts, shares
  results.push(
    await test("CASCADE: delete session removes children", async () => {
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));

      const messages = await db.select().from(MessageTable).where(eq(MessageTable.session_id, sess.id));
      if (messages.length !== 0)
        throw new Error(`Expected 0 messages, got ${messages.length}`);

      // part2 should be gone (cascaded via message)
      const parts = await db.select().from(PartTable).where(eq(PartTable.id, part2.id));
      if (parts.length !== 0)
        throw new Error(`Expected 0 parts, got ${parts.length}`);

      const shares = await db
        .select()
        .from(SessionShareTable)
        .where(eq(SessionShareTable.session_id, sess.id));
      if (shares.length !== 0)
        throw new Error(`Expected 0 shares, got ${shares.length}`);
    })
  );

  // CASCADE: delete project → should cascade to sessions (already empty, but verify constraint)
  results.push(
    await test("CASCADE: delete project", async () => {
      // Re-seed a session under the project to verify cascade
      const sess2 = makeSession(proj.id);
      await db.insert(SessionTable).values(sess2);

      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));

      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id));
      if (sessions.length !== 0)
        throw new Error(`Expected 0 sessions after project delete, got ${sessions.length}`);
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
