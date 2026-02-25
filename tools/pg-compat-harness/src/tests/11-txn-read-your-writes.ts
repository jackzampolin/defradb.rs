// 11-txn-read-your-writes: Verify that mutations within a transaction can see
// documents created earlier in the same transaction. This exercises the
// fetcher_override plumbing in the query runner's mutation pipeline.

import { db, sql } from "../db";
import { eq, and } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  PartTable,
  TodoTable,
} from "../schema";
import {
  makeProject,
  makeSession,
  makeMessage,
  makeTextPart,
  makeTodo,
} from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // INSERT then UPDATE in same transaction (the core bug this feature fixes)
  results.push(
    await test("insert then update in same txn", async () => {
      const proj = makeProject({ name: "ryw-update-test" });
      const sess = makeSession(proj.id, { title: "Original title" });

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx
          .update(SessionTable)
          .set({ title: "Updated in same txn" })
          .where(eq(SessionTable.id, sess.id));
      });

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length}`);
      if (rows[0].title !== "Updated in same txn")
        throw new Error(`Expected "Updated in same txn", got "${rows[0].title}"`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // INSERT then DELETE in same transaction
  results.push(
    await test("insert then delete in same txn", async () => {
      const proj = makeProject({ name: "ryw-delete-test" });
      const sess = makeSession(proj.id);

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      });

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows.length !== 0)
        throw new Error(`Expected 0 rows after delete, got ${rows.length}`);

      // Cleanup
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // INSERT then SELECT in same transaction (read-your-own-writes for queries)
  results.push(
    await test("insert then select in same txn", async () => {
      const proj = makeProject({ name: "ryw-select-test" });
      const sess = makeSession(proj.id, { title: "Visible in txn" });

      let foundInTxn = false;
      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        const rows = await tx
          .select()
          .from(SessionTable)
          .where(eq(SessionTable.id, sess.id));
        foundInTxn = rows.length === 1 && rows[0].title === "Visible in txn";
      });

      if (!foundInTxn)
        throw new Error("Inserted row not visible in same transaction");

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Multi-table: insert parent and child, then update child via filter
  results.push(
    await test("insert parent+child then update child in same txn", async () => {
      const proj = makeProject({ name: "ryw-multi-table" });
      const sess = makeSession(proj.id, { title: "Child session" });
      const msg = makeMessage(sess.id, "user");

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx.insert(MessageTable).values(msg);
        // Update session title after inserting child message
        await tx
          .update(SessionTable)
          .set({ title: "After message insert" })
          .where(eq(SessionTable.id, sess.id));
      });

      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (sessions[0]?.title !== "After message insert")
        throw new Error(`Title mismatch: "${sessions[0]?.title}"`);

      const messages = await db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.session_id, sess.id));
      if (messages.length !== 1)
        throw new Error(`Expected 1 message, got ${messages.length}`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Multiple updates to same row in one transaction
  results.push(
    await test("multiple updates to same row in txn", async () => {
      const proj = makeProject({ name: "ryw-multi-update" });
      const sess = makeSession(proj.id, { title: "v1" });

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx
          .update(SessionTable)
          .set({ title: "v2" })
          .where(eq(SessionTable.id, sess.id));
        await tx
          .update(SessionTable)
          .set({ title: "v3" })
          .where(eq(SessionTable.id, sess.id));
      });

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0]?.title !== "v3")
        throw new Error(`Expected "v3", got "${rows[0]?.title}"`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Insert multiple rows then update with filter matching subset
  results.push(
    await test("insert multiple then filter-update in txn", async () => {
      const proj = makeProject({ name: "ryw-filter-update" });
      const sess1 = makeSession(proj.id, { title: "keep" });
      const sess2 = makeSession(proj.id, { title: "change-me" });
      const sess3 = makeSession(proj.id, { title: "keep" });

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values([sess1, sess2, sess3]);
        await tx
          .update(SessionTable)
          .set({ title: "changed" })
          .where(eq(SessionTable.id, sess2.id));
      });

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id));

      const titles = rows.map((r) => r.title).sort();
      if (!titles.includes("changed"))
        throw new Error(`"changed" not found in titles: ${JSON.stringify(titles)}`);

      const keepCount = titles.filter((t) => t === "keep").length;
      if (keepCount !== 2)
        throw new Error(`Expected 2 "keep" rows, got ${keepCount}`);

      // Cleanup
      for (const s of [sess1, sess2, sess3]) {
        await db.delete(SessionTable).where(eq(SessionTable.id, s.id));
      }
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Insert then update with RETURNING
  results.push(
    await test("insert then update with RETURNING in txn", async () => {
      const proj = makeProject({ name: "ryw-returning" });
      const sess = makeSession(proj.id, { title: "before" });

      let returnedTitle: string | null = null;
      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        const updated = await tx
          .update(SessionTable)
          .set({ title: "after" })
          .where(eq(SessionTable.id, sess.id))
          .returning({ title: SessionTable.title });
        returnedTitle = updated[0]?.title ?? null;
      });

      if (returnedTitle !== "after")
        throw new Error(`RETURNING title: "${returnedTitle}", expected "after"`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Insert todo (composite PK) then update in same txn
  results.push(
    await test("insert composite PK then update in txn", async () => {
      const proj = makeProject({ name: "ryw-composite" });
      const sess = makeSession(proj.id);
      const todo = makeTodo(sess.id, 1, { status: "pending" });

      await db.transaction(async (tx) => {
        await tx.insert(ProjectTable).values(proj);
        await tx.insert(SessionTable).values(sess);
        await tx.insert(TodoTable).values(todo);
        await tx
          .update(TodoTable)
          .set({ status: "completed" })
          .where(
            and(
              eq(TodoTable.session_id, sess.id),
              eq(TodoTable.position, 1)
            )
          );
      });

      const todos = await db
        .select()
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id));
      if (todos[0]?.status !== "completed")
        throw new Error(`Expected "completed", got "${todos[0]?.status}"`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
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
