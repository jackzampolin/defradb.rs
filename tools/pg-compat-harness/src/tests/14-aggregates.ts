// 14-aggregates: COUNT, SUM, AVG, MIN, MAX aggregate queries via Drizzle.

import { db } from "../db";
import { count, sum, avg, min, max, eq, inArray } from "drizzle-orm";
import { SessionTable, TodoTable, ProjectTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: 1 project, 3 sessions, 5 todos with varying positions
  const proj = makeProject({ name: "agg-test-project" });
  await db.insert(ProjectTable).values(proj);

  const sessions = [
    makeSession(proj.id, { title: "Agg session 1", summary_additions: 10, summary_deletions: 5 }),
    makeSession(proj.id, { title: "Agg session 2", summary_additions: 20, summary_deletions: 15 }),
    makeSession(proj.id, { title: "Agg session 3", summary_additions: 30, summary_deletions: 25 }),
  ];
  await db.insert(SessionTable).values(sessions);

  const sessionIds = sessions.map((s) => s.id);

  const todos = [
    makeTodo(sessions[0].id, 1, { priority: "high" }),
    makeTodo(sessions[0].id, 2, { priority: "medium" }),
    makeTodo(sessions[0].id, 3, { priority: "low" }),
    makeTodo(sessions[1].id, 1, { priority: "high" }),
    makeTodo(sessions[1].id, 2, { priority: "medium" }),
  ];
  await db.insert(TodoTable).values(todos);

  // COUNT(*) — filter to our sessions to avoid counting data from other categories
  results.push(
    await test("COUNT(*) all todos", async () => {
      const result = await db
        .select({ cnt: count() })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].cnt) !== 5) throw new Error(`Expected count=5, got ${result[0].cnt}`);
    })
  );

  // COUNT(*) with WHERE filter
  results.push(
    await test("COUNT(*) with WHERE", async () => {
      const result = await db
        .select({ cnt: count() })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sessions[0].id));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].cnt) !== 3) throw new Error(`Expected count=3, got ${result[0].cnt}`);
    })
  );

  // SUM(column)
  results.push(
    await test("SUM(position)", async () => {
      const result = await db
        .select({ total: sum(TodoTable.position) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      // positions: 1+2+3+1+2 = 9
      if (Number(result[0].total) !== 9) throw new Error(`Expected sum=9, got ${result[0].total}`);
    })
  );

  // AVG(column)
  results.push(
    await test("AVG(position)", async () => {
      const result = await db
        .select({ average: avg(TodoTable.position) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      const avgVal = Number(result[0].average);
      // avg of 1,2,3,1,2 = 1.8
      if (Math.abs(avgVal - 1.8) > 0.1) throw new Error(`Expected avg~1.8, got ${avgVal}`);
    })
  );

  // MIN(column)
  results.push(
    await test("MIN(position)", async () => {
      const result = await db
        .select({ minimum: min(TodoTable.position) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].minimum) !== 1) throw new Error(`Expected min=1, got ${result[0].minimum}`);
    })
  );

  // MAX(column)
  results.push(
    await test("MAX(position)", async () => {
      const result = await db
        .select({ maximum: max(TodoTable.position) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].maximum) !== 3) throw new Error(`Expected max=3, got ${result[0].maximum}`);
    })
  );

  // COUNT(*) on empty result
  results.push(
    await test("COUNT(*) empty result", async () => {
      const result = await db
        .select({ cnt: count() })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, "nonexistent"));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].cnt) !== 0) throw new Error(`Expected count=0, got ${result[0].cnt}`);
    })
  );

  // Multiple aggregates in one query
  results.push(
    await test("multiple aggregates (count + sum)", async () => {
      const result = await db
        .select({ cnt: count(), total: sum(TodoTable.position) })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds));
      if (result.length !== 1) throw new Error(`Expected 1 row, got ${result.length}`);
      if (Number(result[0].cnt) !== 5) throw new Error(`Expected count=5, got ${result[0].cnt}`);
      if (Number(result[0].total) !== 9) throw new Error(`Expected sum=9, got ${result[0].total}`);
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
