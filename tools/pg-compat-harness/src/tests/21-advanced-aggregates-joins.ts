// 21-advanced-aggregates-joins: COUNT(DISTINCT), compound HAVING, multi-table JOINs.

import { db } from "../db";
import {
  eq,
  and,
  gt,
  lt,
  count,
  countDistinct,
  sum,
  inArray,
} from "drizzle-orm";
import { ProjectTable, SessionTable, MessageTable, TodoTable } from "../schema";
import { makeProject, makeSession, makeMessage, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: 2 projects, sessions with varying attributes, messages, todos
  const projects = [
    makeProject({ name: "AdvAgg project A" }),
    makeProject({ name: "AdvAgg project B" }),
  ];
  await db.insert(ProjectTable).values(projects);

  const projectIds = projects.map((p) => p.id);

  const sessions = [
    makeSession(projects[0].id, { title: "AA-sess-1", summary_additions: 10 }),
    makeSession(projects[0].id, { title: "AA-sess-2", summary_additions: 20 }),
    makeSession(projects[0].id, { title: "AA-sess-3", summary_additions: 30 }),
    makeSession(projects[1].id, { title: "AB-sess-1", summary_additions: 40 }),
    makeSession(projects[1].id, { title: "AB-sess-2", summary_additions: 50 }),
  ];
  await db.insert(SessionTable).values(sessions);

  const sessionIds = sessions.map((s) => s.id);

  // Messages for join tests
  const messages = [
    makeMessage(sessions[0].id),
    makeMessage(sessions[0].id),
    makeMessage(sessions[1].id),
    makeMessage(sessions[3].id),
  ];
  await db.insert(MessageTable).values(messages);

  // Todos with varying statuses for COUNT(DISTINCT)
  const todos = [
    makeTodo(sessions[0].id, 1, { status: "pending", priority: "high" }),
    makeTodo(sessions[0].id, 2, { status: "done", priority: "medium" }),
    makeTodo(sessions[0].id, 3, { status: "pending", priority: "low" }),
    makeTodo(sessions[1].id, 1, { status: "done", priority: "high" }),
    makeTodo(sessions[1].id, 2, { status: "in_progress", priority: "medium" }),
    makeTodo(sessions[2].id, 1, { status: "pending", priority: "high" }),
  ];
  await db.insert(TodoTable).values(todos);

  const todoSessionIds = [sessions[0].id, sessions[1].id, sessions[2].id];

  // COUNT(DISTINCT col) on status field
  results.push(
    await test("COUNT(DISTINCT status)", async () => {
      const result = await db
        .select({ cnt: countDistinct(TodoTable.status) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, todoSessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      // statuses: pending, done, in_progress → 3 distinct
      if (Number(result[0].cnt) !== 3)
        throw new Error(`Expected 3 distinct statuses, got ${result[0].cnt}`);
    })
  );

  // COUNT(DISTINCT) with WHERE filter
  results.push(
    await test("COUNT(DISTINCT) with WHERE", async () => {
      const result = await db
        .select({ cnt: countDistinct(TodoTable.status) })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sessions[0].id));
      // session 0 statuses: pending, done → 2 distinct
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].cnt) !== 2)
        throw new Error(`Expected 2 distinct statuses, got ${result[0].cnt}`);
    })
  );

  // Multiple aggregates with aliases
  results.push(
    await test("multiple aggregates with aliases", async () => {
      const result = await db
        .select({
          total: count(),
          posSum: sum(TodoTable.position),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, todoSessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].total) !== 6)
        throw new Error(`Expected count=6, got ${result[0].total}`);
      // positions: 1+2+3+1+2+1 = 10
      if (Number(result[0].posSum) !== 10)
        throw new Error(`Expected sum=10, got ${result[0].posSum}`);
    })
  );

  // Compound HAVING (count > 1 AND count < 4)
  results.push(
    await test("compound HAVING (AND)", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          cnt: count(),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, todoSessionIds))
        .groupBy(TodoTable.session_id)
        .having(and(gt(count(), 1), lt(count(), 4)));
      // session 0: 3 todos (passes 1<3<4), session 1: 2 (passes), session 2: 1 (fails >1)
      if (result.length !== 2)
        throw new Error(`Expected 2 groups, got ${result.length}`);
    })
  );

  // Three-table JOIN chain (project → session → message)
  results.push(
    await test("three-table JOIN chain", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
          messageId: MessageTable.id,
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .innerJoin(MessageTable, eq(SessionTable.id, MessageTable.session_id))
        .where(inArray(ProjectTable.id, projectIds));
      // Project A: sess-1 has 2 msgs, sess-2 has 1 msg = 3; Project B: sess-1 has 1 msg = 1; total = 4
      if (rows.length !== 4) throw new Error(`Expected 4 rows, got ${rows.length}`);
    })
  );

  // LEFT JOIN three tables (with nulls)
  results.push(
    await test("LEFT JOIN three tables", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionTitle: SessionTable.title,
          messageId: MessageTable.id,
        })
        .from(ProjectTable)
        .leftJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .leftJoin(MessageTable, eq(SessionTable.id, MessageTable.session_id))
        .where(inArray(ProjectTable.id, projectIds));
      // Project A: sess-1(2 msgs) + sess-2(1 msg) + sess-3(0 msgs→null) = 4
      // Project B: sess-1(1 msg) + sess-2(0 msgs→null) = 2
      // total = 6
      if (rows.length !== 6) throw new Error(`Expected 6 rows, got ${rows.length}`);
    })
  );

  // JOIN + GROUP BY (count sessions per project)
  results.push(
    await test("JOIN + GROUP BY sessions per project", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          sessionCount: count(),
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .where(inArray(ProjectTable.id, projectIds))
        .groupBy(ProjectTable.name);
      if (rows.length !== 2) throw new Error(`Expected 2 groups, got ${rows.length}`);
      for (const row of rows) {
        const expected = row.projectName === "AdvAgg project A" ? 3 : 2;
        if (Number(row.sessionCount) !== expected)
          throw new Error(
            `${row.projectName}: expected ${expected} sessions, got ${row.sessionCount}`
          );
      }
    })
  );

  // JOIN + aggregate with alias
  results.push(
    await test("JOIN + aggregate with alias", async () => {
      const rows = await db
        .select({
          projectName: ProjectTable.name,
          msgCount: count(),
        })
        .from(ProjectTable)
        .innerJoin(SessionTable, eq(ProjectTable.id, SessionTable.project_id))
        .innerJoin(MessageTable, eq(SessionTable.id, MessageTable.session_id))
        .where(inArray(ProjectTable.id, projectIds))
        .groupBy(ProjectTable.name);
      if (rows.length !== 2) throw new Error(`Expected 2 groups, got ${rows.length}`);
      for (const row of rows) {
        const expected = row.projectName === "AdvAgg project A" ? 3 : 1;
        if (Number(row.msgCount) !== expected)
          throw new Error(
            `${row.projectName}: expected ${expected} messages, got ${row.msgCount}`
          );
      }
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
