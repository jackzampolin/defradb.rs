// 08-pagination: Streaming pagination with LIMIT/OFFSET loop.
// OpenCode loads sessions in pages of 50.

import { db } from "../db";
import { eq, desc } from "drizzle-orm";
import { ProjectTable, SessionTable } from "../schema";
import { makeProject, makeSessions } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed 120 sessions
  const proj = makeProject({ name: "pagination-test" });
  await db.insert(ProjectTable).values(proj);
  const sessions = makeSessions(proj.id, 120);
  // Insert in batches (some PG drivers limit param count)
  for (let i = 0; i < sessions.length; i += 40) {
    await db.insert(SessionTable).values(sessions.slice(i, i + 40));
  }

  // Page through all sessions
  results.push(
    await test("paginate with LIMIT 50", async () => {
      const pageSize = 50;
      let allRows: any[] = [];
      let offset = 0;

      while (true) {
        const page = await db
          .select()
          .from(SessionTable)
          .where(eq(SessionTable.project_id, proj.id))
          .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id))
          .limit(pageSize)
          .offset(offset);

        allRows = allRows.concat(page);
        if (page.length < pageSize) break;
        offset += pageSize;
      }

      if (allRows.length !== 120)
        throw new Error(`Expected 120, got ${allRows.length}`);
    })
  );

  // Verify order is consistent across pages
  results.push(
    await test("pagination order consistency", async () => {
      const page1 = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id))
        .limit(50)
        .offset(0);

      const page2 = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, proj.id))
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id))
        .limit(50)
        .offset(50);

      // Last item of page1 should have higher time_updated than first of page2
      const last1 = page1[page1.length - 1];
      const first2 = page2[0];
      if (last1.time_updated < first2.time_updated)
        throw new Error("Order inconsistency across pages");

      // No overlap
      const page1Ids = new Set(page1.map((r) => r.id));
      const overlap = page2.filter((r) => page1Ids.has(r.id));
      if (overlap.length > 0) throw new Error("Pages overlap");
    })
  );

  // Small page size stress
  results.push(
    await test("paginate with small page (10)", async () => {
      let count = 0;
      let offset = 0;
      const pageSize = 10;

      while (true) {
        const page = await db
          .select({ id: SessionTable.id })
          .from(SessionTable)
          .where(eq(SessionTable.project_id, proj.id))
          .orderBy(desc(SessionTable.time_updated))
          .limit(pageSize)
          .offset(offset);
        count += page.length;
        if (page.length < pageSize) break;
        offset += pageSize;
      }

      if (count !== 120) throw new Error(`Expected 120, got ${count}`);
    })
  );

  // Cleanup
  await db.delete(SessionTable).where(eq(SessionTable.project_id, proj.id));
  await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));

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
