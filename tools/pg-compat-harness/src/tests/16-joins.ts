// 16-joins: INNER JOIN and LEFT JOIN via Drizzle select builder.

import { db } from "../db";
import { eq, inArray } from "drizzle-orm";
import { ProjectTable, SessionTable, MessageTable } from "../schema";
import { makeProject, makeSession, makeMessage } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: 3 projects, varying sessions
  const projects = [
    makeProject({ name: "Join project A" }),
    makeProject({ name: "Join project B" }),
    makeProject({ name: "Join project C (no sessions)" }),
  ];
  await db.insert(ProjectTable).values(projects);

  const projectIds = projects.map((p) => p.id);

  const sessions = [
    makeSession(projects[0].id, { title: "A-session-1", time_updated: 1000 }),
    makeSession(projects[0].id, { title: "A-session-2", time_updated: 2000 }),
    makeSession(projects[1].id, { title: "B-session-1", time_updated: 3000 }),
  ];
  await db.insert(SessionTable).values(sessions);

  const sessionIds = sessions.map((s) => s.id);

  const messages = [
    makeMessage(sessions[0].id),
    makeMessage(sessions[1].id),
    makeMessage(sessions[2].id),
  ];
  await db.insert(MessageTable).values(messages);

  // INNER JOIN project → session (filter to our projects)
  results.push(
    await test("INNER JOIN project-session", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(inArray(ProjectTable.id, projectIds));
      // Project A has 2 sessions, Project B has 1, Project C has 0
      if (rows.length !== 3) throw new Error(`Expected 3 rows, got ${rows.length}`);
    })
  );

  // LEFT JOIN (all our projects, nullable sessions)
  results.push(
    await test("LEFT JOIN project-session", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
        })
        .from(ProjectTable)
        .leftJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(inArray(ProjectTable.id, projectIds));
      // 3 projects: A(2 sessions) + B(1 session) + C(null) = 4 rows
      if (rows.length !== 4) throw new Error(`Expected 4 rows, got ${rows.length}`);
      // Project C should have null session title
      const projectC = rows.find((r) => r.projectName === "Join project C (no sessions)");
      if (projectC && projectC.sessionTitle !== null) {
        throw new Error("Expected null session title for project C");
      }
    })
  );

  // JOIN with WHERE filter on primary table
  results.push(
    await test("JOIN with WHERE on primary", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(eq(ProjectTable.name, "Join project A"));
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // JOIN with column subset (filter to our projects)
  results.push(
    await test("JOIN with column subset", async () => {
      const rows = await db
        .select({
          pid: ProjectTable.id,
          sid: SessionTable.id,
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(inArray(ProjectTable.id, projectIds));
      if (rows.length !== 3) throw new Error(`Expected 3 rows, got ${rows.length}`);
      const row = rows[0] as Record<string, unknown>;
      if (!row.pid || !row.sid) throw new Error("Missing expected columns");
    })
  );

  // JOIN with LIMIT (filter to our projects)
  results.push(
    await test("JOIN with LIMIT", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(inArray(ProjectTable.id, projectIds))
        .limit(2);
      if (rows.length !== 2) throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // Multi-table JOIN session → message (filter to our sessions)
  results.push(
    await test("JOIN session-message", async () => {
      const rows = await db
        .select({
          sessionTitle: SessionTable.title,
          messageId: MessageTable.id,
        })
        .from(SessionTable)
        .innerJoin(MessageTable, eq(SessionTable.id, MessageTable.session_id))
        .where(inArray(SessionTable.id, sessionIds));
      if (rows.length !== 3) throw new Error(`Expected 3 rows, got ${rows.length}`);
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
