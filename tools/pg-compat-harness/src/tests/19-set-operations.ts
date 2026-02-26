// 19-set-operations: UNION, UNION ALL, INTERSECT, EXCEPT via Drizzle.

import { db } from "../db";
import { eq, inArray } from "drizzle-orm";
import { union, unionAll, intersect, except } from "drizzle-orm/pg-core";
import { ProjectTable, SessionTable } from "../schema";
import { makeProject, makeSession } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: projects for set operations
  const projects = [
    makeProject({ name: "Set proj A" }),
    makeProject({ name: "Set proj B" }),
    makeProject({ name: "Set proj C" }),
  ];
  await db.insert(ProjectTable).values(projects);

  // UNION (deduped) — combine two SELECT queries
  results.push(
    await test("UNION deduped", async () => {
      const q1 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[0].id));
      const q2 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[1].id));
      const rows = await union(q1, q2);
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // UNION ALL (with duplicates)
  results.push(
    await test("UNION ALL with duplicates", async () => {
      const q1 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[0].id));
      const q2 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[0].id));
      const rows = await unionAll(q1, q2);
      // Same project selected twice → 2 rows
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // INTERSECT — use ID-based filters for deterministic results
  results.push(
    await test("INTERSECT", async () => {
      const projectIds = projects.map((p) => p.id);
      const q1 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(inArray(ProjectTable.id, projectIds));
      const q2 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[0].id));
      const rows = await intersect(q1, q2);
      // Only project A is in both
      if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length}`);
    })
  );

  // EXCEPT
  results.push(
    await test("EXCEPT", async () => {
      const projectIds = projects.map((p) => p.id);
      const q1 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(inArray(ProjectTable.id, projectIds));
      const q2 = db
        .select({ id: ProjectTable.id, name: ProjectTable.name })
        .from(ProjectTable)
        .where(eq(ProjectTable.id, projects[0].id));
      const rows = await except(q1, q2);
      // All our projects except A → B and C
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
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
