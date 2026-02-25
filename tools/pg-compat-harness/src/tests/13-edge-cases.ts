// 13-edge-cases: Boundary conditions and patterns that stress the SQL→GraphQL
// bridge and encoding layer. Empty results, NULL updates, range queries,
// large parameter lists, RETURNING from DELETE, and mixed-type operations.

import { db, sql } from "../db";
import { eq, and, gte, lte, inArray, desc } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  TodoTable,
} from "../schema";
import {
  makeProject,
  makeSession,
  makeMessage,
  makeTodo,
} from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Empty result set — query that matches nothing
  results.push(
    await test("empty result set", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, "nonexistent-id-that-does-not-exist"));
      if (rows.length !== 0)
        throw new Error(`Expected 0 rows, got ${rows.length}`);
    })
  );

  // UPDATE that matches nothing — should succeed with 0 affected rows
  results.push(
    await test("update matching nothing", async () => {
      // This should not error, just affect 0 rows
      await db
        .update(SessionTable)
        .set({ title: "ghost" })
        .where(eq(SessionTable.id, "nonexistent-id"));
      // No error means pass
    })
  );

  // DELETE that matches nothing
  results.push(
    await test("delete matching nothing", async () => {
      await db
        .delete(SessionTable)
        .where(eq(SessionTable.id, "nonexistent-id"));
    })
  );

  // Range query using GTE + LTE (equivalent to BETWEEN)
  results.push(
    await test("range query with GTE + LTE", async () => {
      const proj = makeProject({ name: "edge-range" });
      await db.insert(ProjectTable).values(proj);

      const sessions = Array.from({ length: 5 }, (_, i) =>
        makeSession(proj.id, {
          title: `Range-${i}`,
          summary_additions: (i + 1) * 10,
        })
      );
      await db.insert(SessionTable).values(sessions);

      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            gte(SessionTable.summary_additions, 20),
            lte(SessionTable.summary_additions, 40)
          )
        );
      // summary_additions: 20, 30, 40
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows in range [20,40], got ${rows.length}`);

      // Cleanup
      for (const s of sessions) {
        await db.delete(SessionTable).where(eq(SessionTable.id, s.id));
      }
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Large IN list — 20+ items
  results.push(
    await test("large IN list (20 items)", async () => {
      const proj = makeProject({ name: "edge-large-in" });
      await db.insert(ProjectTable).values(proj);

      const sessions = Array.from({ length: 25 }, (_, i) =>
        makeSession(proj.id, { title: `LargeIn-${i}` })
      );
      await db.insert(SessionTable).values(sessions);

      // Query first 20 by ID
      const targetIds = sessions.slice(0, 20).map((s) => s.id);
      const rows = await db
        .select()
        .from(SessionTable)
        .where(inArray(SessionTable.id, targetIds));

      if (rows.length !== 20)
        throw new Error(`Expected 20 rows from IN list, got ${rows.length}`);

      // Cleanup
      for (const s of sessions) {
        await db.delete(SessionTable).where(eq(SessionTable.id, s.id));
      }
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // NULL column updates — set to null, then set back to value
  results.push(
    await test("set null then set back to value", async () => {
      const proj = makeProject({ name: "edge-null-roundtrip" });
      const sess = makeSession(proj.id, {
        title: "null-test",
        share_url: "https://example.com/share",
      });
      await db.insert(ProjectTable).values(proj);
      await db.insert(SessionTable).values(sess);

      // Verify initial non-null
      let rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0]?.share_url !== "https://example.com/share")
        throw new Error("Initial share_url wrong");

      // Set to null
      await db
        .update(SessionTable)
        .set({ share_url: null })
        .where(eq(SessionTable.id, sess.id));

      rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0]?.share_url !== null)
        throw new Error(`Expected null, got "${rows[0]?.share_url}"`);

      // Set back to value
      await db
        .update(SessionTable)
        .set({ share_url: "https://example.com/restored" })
        .where(eq(SessionTable.id, sess.id));

      rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0]?.share_url !== "https://example.com/restored")
        throw new Error(`Expected restored URL, got "${rows[0]?.share_url}"`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // DELETE with RETURNING
  results.push(
    await test("delete with RETURNING", async () => {
      const proj = makeProject({ name: "edge-delete-returning" });
      const sess = makeSession(proj.id, { title: "to-delete" });
      await db.insert(ProjectTable).values(proj);
      await db.insert(SessionTable).values(sess);

      const deleted = await db
        .delete(SessionTable)
        .where(eq(SessionTable.id, sess.id))
        .returning({ id: SessionTable.id, title: SessionTable.title });

      if (deleted.length !== 1)
        throw new Error(`Expected 1 deleted row, got ${deleted.length}`);
      if (deleted[0].title !== "to-delete")
        throw new Error(`Wrong RETURNING title: "${deleted[0].title}"`);

      // Verify actually deleted
      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows.length !== 0)
        throw new Error("Row still exists after delete");

      // Cleanup
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Multi-row INSERT with RETURNING
  results.push(
    await test("multi-row insert with RETURNING", async () => {
      const proj = makeProject({ name: "edge-multi-returning" });
      await db.insert(ProjectTable).values(proj);

      const sessions = Array.from({ length: 3 }, (_, i) =>
        makeSession(proj.id, { title: `Multi-${i}` })
      );

      const inserted = await db
        .insert(SessionTable)
        .values(sessions)
        .returning({ id: SessionTable.id, title: SessionTable.title });

      if (inserted.length !== 3)
        throw new Error(`Expected 3 returned rows, got ${inserted.length}`);

      const titles = inserted.map((r) => r.title).sort();
      if (titles[0] !== "Multi-0" || titles[2] !== "Multi-2")
        throw new Error(`Wrong returned titles: ${JSON.stringify(titles)}`);

      // Cleanup
      for (const s of sessions) {
        await db.delete(SessionTable).where(eq(SessionTable.id, s.id));
      }
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Update multiple fields including bigint and nullable text
  results.push(
    await test("update multiple field types simultaneously", async () => {
      const proj = makeProject({ name: "edge-multi-field" });
      const sess = makeSession(proj.id, {
        title: "original",
        summary_additions: null,
        summary_deletions: null,
        summary_files: null,
        share_url: null,
      });
      await db.insert(ProjectTable).values(proj);
      await db.insert(SessionTable).values(sess);

      await db
        .update(SessionTable)
        .set({
          title: "updated",
          summary_additions: 100,
          summary_deletions: 50,
          summary_files: 5,
          share_url: "https://example.com/updated",
        })
        .where(eq(SessionTable.id, sess.id));

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));

      const row = rows[0];
      if (!row) throw new Error("Row not found");
      if (row.title !== "updated") throw new Error(`title: ${row.title}`);
      if (row.summary_additions !== 100) throw new Error(`additions: ${row.summary_additions}`);
      if (row.summary_deletions !== 50) throw new Error(`deletions: ${row.summary_deletions}`);
      if (row.summary_files !== 5) throw new Error(`files: ${row.summary_files}`);
      if (row.share_url !== "https://example.com/updated")
        throw new Error(`share_url: ${row.share_url}`);

      // Cleanup
      await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Ordering with NULL values — NULLs sort last in DefraDB
  results.push(
    await test("ordering with NULL values", async () => {
      const proj = makeProject({ name: "edge-null-order" });
      await db.insert(ProjectTable).values(proj);

      const sessions = [
        makeSession(proj.id, { title: "Has-additions", summary_additions: 100 }),
        makeSession(proj.id, { title: "No-additions", summary_additions: null }),
        makeSession(proj.id, { title: "Small-additions", summary_additions: 10 }),
      ];
      await db.insert(SessionTable).values(sessions);

      const rows = await db
        .select({ title: SessionTable.title, additions: SessionTable.summary_additions })
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.summary_additions));

      // Just verify we get all 3 rows and the query doesn't crash
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows, got ${rows.length}`);

      // Cleanup
      for (const s of sessions) {
        await db.delete(SessionTable).where(eq(SessionTable.id, s.id));
      }
      await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));
    })
  );

  // Special characters in text values
  results.push(
    await test("special characters in text values", async () => {
      const proj = makeProject({ name: "edge-special-chars" });
      const specialTitle = `Session with "quotes" and 'apostrophes' and \\ backslashes`;
      const sess = makeSession(proj.id, { title: specialTitle });
      await db.insert(ProjectTable).values(proj);
      await db.insert(SessionTable).values(sess);

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      if (rows[0]?.title !== specialTitle)
        throw new Error(`Title mismatch: ${JSON.stringify(rows[0]?.title)}`);

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
