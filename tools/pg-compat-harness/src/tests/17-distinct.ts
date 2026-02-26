// 17-distinct: SELECT DISTINCT queries via Drizzle.

import { db } from "../db";
import { eq, and, asc } from "drizzle-orm";
import { TodoTable, SessionTable, ProjectTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: todos with overlapping status/priority values
  const proj = makeProject({ name: "distinct-test-project" });
  await db.insert(ProjectTable).values(proj);

  const sess = makeSession(proj.id, { title: "Distinct session" });
  await db.insert(SessionTable).values(sess);

  const todos = [
    makeTodo(sess.id, 1, { status: "pending", priority: "high" }),
    makeTodo(sess.id, 2, { status: "done", priority: "high" }),
    makeTodo(sess.id, 3, { status: "pending", priority: "low" }),
    makeTodo(sess.id, 4, { status: "done", priority: "low" }),
    makeTodo(sess.id, 5, { status: "pending", priority: "high" }),  // duplicate status+priority
  ];
  await db.insert(TodoTable).values(todos);

  // SELECT DISTINCT single column
  results.push(
    await test("SELECT DISTINCT status", async () => {
      const rows = await db
        .selectDistinct({ status: TodoTable.status })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id));
      // Should get "pending" and "done" = 2 distinct values
      if (rows.length !== 2) throw new Error(`Expected 2 distinct, got ${rows.length}`);
    })
  );

  // SELECT DISTINCT multiple columns
  results.push(
    await test("SELECT DISTINCT status, priority", async () => {
      const rows = await db
        .selectDistinct({
          status: TodoTable.status,
          priority: TodoTable.priority,
        })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id));
      // pending+high, done+high, pending+low, done+low = 4 combos
      if (rows.length !== 4) throw new Error(`Expected 4 distinct, got ${rows.length}`);
    })
  );

  // SELECT DISTINCT with ORDER BY
  results.push(
    await test("SELECT DISTINCT with ORDER BY", async () => {
      const rows = await db
        .selectDistinct({ status: TodoTable.status })
        .from(TodoTable)
        .where(eq(TodoTable.session_id, sess.id))
        .orderBy(asc(TodoTable.status));
      if (rows.length !== 2) throw new Error(`Expected 2 distinct, got ${rows.length}`);
      // "done" should come before "pending" in ASC order
      if (rows[0].status !== "done") throw new Error(`Expected "done" first, got "${rows[0].status}"`);
    })
  );

  // SELECT DISTINCT with WHERE (filter to our session to avoid accumulation)
  results.push(
    await test("SELECT DISTINCT priority with WHERE status=pending", async () => {
      const rows = await db
        .selectDistinct({ priority: TodoTable.priority })
        .from(TodoTable)
        .where(and(eq(TodoTable.status, "pending"), eq(TodoTable.session_id, sess.id)));
      // pending+high (x2) and pending+low → 2 distinct priorities
      if (rows.length !== 2) throw new Error(`Expected 2 distinct, got ${rows.length}`);
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
