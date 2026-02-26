// 03-insert: Basic INSERT, INSERT with RETURNING, upsert (ON CONFLICT DO UPDATE).
// Exercises the Drizzle ORM insert patterns OpenCode uses.

import { db } from "../db";
import { eq } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  PartTable,
  TodoTable,
  PermissionTable,
  SessionShareTable,
} from "../schema";
import {
  makeProject,
  makeSession,
  makeMessage,
  makeTextPart,
  makeTodo,
  makePermission,
  makeSessionShare,
} from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  const proj = makeProject();
  const sess = makeSession(proj.id);
  const msg = makeMessage(sess.id, "user");
  const part = makeTextPart(msg.id, sess.id, "Hello, can you help me?");

  // Basic insert
  results.push(
    await test("insert project", async () => {
      await db.insert(ProjectTable).values(proj);
    })
  );

  results.push(
    await test("insert session (FK to project)", async () => {
      await db.insert(SessionTable).values(sess);
    })
  );

  results.push(
    await test("insert message (FK to session)", async () => {
      await db.insert(MessageTable).values(msg);
    })
  );

  results.push(
    await test("insert part (FK to message)", async () => {
      await db.insert(PartTable).values(part);
    })
  );

  // Composite PK insert
  results.push(
    await test("insert todo (composite PK)", async () => {
      const todo = makeTodo(sess.id, 0);
      await db.insert(TodoTable).values(todo);
    })
  );

  // FK-as-PK insert
  results.push(
    await test("insert permission (FK as PK)", async () => {
      const perm = makePermission(proj.id);
      await db.insert(PermissionTable).values(perm);
    })
  );

  results.push(
    await test("insert session_share", async () => {
      const share = makeSessionShare(sess.id);
      await db.insert(SessionShareTable).values(share);
    })
  );

  // INSERT ... RETURNING (Drizzle uses this pattern)
  results.push(
    await test("insert with returning", async () => {
      const msg2 = makeMessage(sess.id, "assistant");
      const rows = await db.insert(MessageTable).values(msg2).returning();
      if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length}`);
      if (rows[0].id !== msg2.id) throw new Error("Returned id mismatch");
    })
  );

  // Upsert: ON CONFLICT DO UPDATE (OpenCode uses this for session_share)
  results.push(
    await test("upsert (ON CONFLICT DO UPDATE)", async () => {
      const share = makeSessionShare(sess.id, { url: "https://updated.example.com" });
      await db
        .insert(SessionShareTable)
        .values(share)
        .onConflictDoUpdate({
          target: SessionShareTable.session_id,
          set: {
            url: share.url,
            time_updated: share.time_updated,
          },
        });
      const check = await db
        .select()
        .from(SessionShareTable)
        .where(eq(SessionShareTable.session_id, sess.id));
      if (check[0]?.url !== "https://updated.example.com")
        throw new Error("Upsert did not update url");
    })
  );

  // Multi-row insert
  results.push(
    await test("multi-row insert", async () => {
      const todos = [
        makeTodo(sess.id, 1, { content: "Fix auth bug" }),
        makeTodo(sess.id, 2, { content: "Add tests" }),
        makeTodo(sess.id, 3, { content: "Update docs" }),
      ];
      await db.insert(TodoTable).values(todos);
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
