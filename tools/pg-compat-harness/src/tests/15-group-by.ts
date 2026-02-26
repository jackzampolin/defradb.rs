// 15-group-by: GROUP BY with aggregates, HAVING via Drizzle.

import { db } from "../db";
import { count, sum, eq, gt, inArray } from "drizzle-orm";
import { TodoTable, SessionTable, ProjectTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: 1 project, 3 sessions, todos distributed across sessions
  const proj = makeProject({ name: "group-test-project" });
  await db.insert(ProjectTable).values(proj);

  const sessions = [
    makeSession(proj.id, { title: "Group session A" }),
    makeSession(proj.id, { title: "Group session B" }),
    makeSession(proj.id, { title: "Group session C" }),
  ];
  await db.insert(SessionTable).values(sessions);

  const sessionIds = sessions.map((s) => s.id);

  // Session A: 3 todos (positions 1,2,3)
  // Session B: 2 todos (positions 1,2)
  // Session C: 1 todo (position 1)
  const todos = [
    makeTodo(sessions[0].id, 1, { status: "done" }),
    makeTodo(sessions[0].id, 2, { status: "pending" }),
    makeTodo(sessions[0].id, 3, { status: "done" }),
    makeTodo(sessions[1].id, 1, { status: "pending" }),
    makeTodo(sessions[1].id, 2, { status: "done" }),
    makeTodo(sessions[2].id, 1, { status: "pending" }),
  ];
  await db.insert(TodoTable).values(todos);

  // GROUP BY single column with COUNT — filter to our sessions
  results.push(
    await test("GROUP BY session_id with COUNT", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          cnt: count(),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds))
        .groupBy(TodoTable.session_id);
      if (result.length !== 3) throw new Error(`Expected 3 groups, got ${result.length}`);
      for (const row of result) {
        const expected = row.session_id === sessions[0].id ? 3
          : row.session_id === sessions[1].id ? 2
          : 1;
        if (Number(row.cnt) !== expected) {
          throw new Error(`Session ${row.session_id}: expected ${expected}, got ${row.cnt}`);
        }
      }
    })
  );

  // GROUP BY single column with SUM
  results.push(
    await test("GROUP BY session_id with SUM(position)", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          total: sum(TodoTable.position),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds))
        .groupBy(TodoTable.session_id);
      if (result.length !== 3) throw new Error(`Expected 3 groups, got ${result.length}`);
    })
  );

  // GROUP BY multiple columns
  results.push(
    await test("GROUP BY session_id, status", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          status: TodoTable.status,
          cnt: count(),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds))
        .groupBy(TodoTable.session_id, TodoTable.status);
      // A: done=2, pending=1; B: pending=1, done=1; C: pending=1 → 5 groups
      if (result.length < 3) throw new Error(`Expected >=3 groups, got ${result.length}`);
    })
  );

  // GROUP BY with HAVING (count > 1) — use proper Drizzle gt() syntax
  results.push(
    await test("GROUP BY with HAVING count > 1", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          cnt: count(),
        })
        .from(TodoTable)
        .where(inArray(TodoTable.session_id, sessionIds))
        .groupBy(TodoTable.session_id)
        .having(gt(count(), 1));
      // Only sessions A (3) and B (2) should pass
      if (result.length !== 2) throw new Error(`Expected 2 groups, got ${result.length}`);
    })
  );

  // GROUP BY on empty result
  results.push(
    await test("GROUP BY on empty result", async () => {
      const result = await db
        .select({
          session_id: TodoTable.session_id,
          cnt: count(),
        })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, "nonexistent"))
        .groupBy(TodoTable.session_id);
      if (result.length !== 0) throw new Error(`Expected 0 groups, got ${result.length}`);
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
