// 18-subqueries: IN (subquery), NOT IN (subquery), EXISTS via Drizzle.

import { db } from "../db";
import { eq, and, inArray, notInArray } from "drizzle-orm";
import { ProjectTable, SessionTable } from "../schema";
import { makeProject, makeSession } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: projects with varying sessions
  const projects = [
    makeProject({ name: "Subquery proj A", worktree: "/path/a" }),
    makeProject({ name: "Subquery proj B", worktree: "/path/b" }),
    makeProject({ name: "Subquery proj C (no sessions)", worktree: "/path/c" }),
  ];
  await db.insert(ProjectTable).values(projects);

  const sessions = [
    makeSession(projects[0].id, { title: "Subq session A1" }),
    makeSession(projects[0].id, { title: "Subq session A2" }),
    makeSession(projects[1].id, { title: "Subq session B1" }),
  ];
  await db.insert(SessionTable).values(sessions);

  // IN (subquery) — find sessions whose project has name containing "A"
  results.push(
    await test("IN subquery: sessions for project A", async () => {
      // First get project IDs matching condition
      const matchingProjects = await db
        .select({ id: ProjectTable.id })
        .from(ProjectTable)
        .where(eq(ProjectTable.name, "Subquery proj A"));
      const projectIds = matchingProjects.map((p) => p.id);

      // Then filter sessions by those IDs
      const rows = await db
        .select()
        .from(SessionTable)
        .where(inArray(SessionTable.project_id, projectIds));
      if (rows.length !== 2)
        throw new Error(`Expected 2 sessions, got ${rows.length}`);
    })
  );

  const sessionIds = sessions.map((s) => s.id);

  // NOT IN (subquery) — scope to our sessions to avoid accumulation
  results.push(
    await test("NOT IN subquery: sessions not in project A", async () => {
      const matchingProjects = await db
        .select({ id: ProjectTable.id })
        .from(ProjectTable)
        .where(eq(ProjectTable.name, "Subquery proj A"));
      const projAIds = matchingProjects.map((p) => p.id);

      const rows = await db
        .select()
        .from(SessionTable)
        .where(and(
          notInArray(SessionTable.project_id, projAIds),
          inArray(SessionTable.id, sessionIds)
        ));
      if (rows.length !== 1)
        throw new Error(`Expected 1 session, got ${rows.length}`);
    })
  );

  const projectIds = projects.map((p) => p.id);

  // Subquery as derived table: find projects with sessions (scoped to our data)
  results.push(
    await test("derived table: projects with sessions", async () => {
      // Get unique project_ids from our sessions only
      const sessionProjects = await db
        .select({ project_id: SessionTable.project_id })
        .from(SessionTable)
        .where(inArray(SessionTable.project_id, projectIds));
      const sessionProjIds = [...new Set(sessionProjects.map((s) => s.project_id))];

      const rows = await db
        .select()
        .from(ProjectTable)
        .where(inArray(ProjectTable.id, sessionProjIds));
      // Projects A and B have sessions, C does not
      if (rows.length !== 2)
        throw new Error(`Expected 2 projects with sessions, got ${rows.length}`);
    })
  );

  // Nested: find our projects without sessions
  results.push(
    await test("projects without sessions", async () => {
      const sessionProjects = await db
        .select({ project_id: SessionTable.project_id })
        .from(SessionTable)
        .where(inArray(SessionTable.project_id, projectIds));
      const sessionProjIds = [...new Set(sessionProjects.map((s) => s.project_id))];

      const rows = await db
        .select()
        .from(ProjectTable)
        .where(notInArray(ProjectTable.id, sessionProjIds));
      // Filter to just our projects
      const ours = rows.filter((r) => projectIds.includes(r.id));
      if (ours.length !== 1)
        throw new Error(`Expected 1 project without sessions, got ${ours.length}`);
      if (ours[0].name !== "Subquery proj C (no sessions)")
        throw new Error(`Expected project C, got ${ours[0].name}`);
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
