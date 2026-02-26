// 20-extended-filters: ILIKE, BETWEEN, NOT operator patterns via Drizzle.

import { db } from "../db";
import {
  eq,
  and,
  or,
  not,
  ilike,
  notIlike,
  between,
  inArray,
} from "drizzle-orm";
import { ProjectTable, SessionTable, TodoTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed: 1 project, 1 session, 5 todos with varying content/status/priority
  const proj = makeProject({ name: "ext-filter-project" });
  await db.insert(ProjectTable).values(proj);

  const session = makeSession(proj.id, { title: "Ext filter session" });
  await db.insert(SessionTable).values(session);

  const todos = [
    makeTodo(session.id, 1, { content: "Buy Groceries", status: "pending", priority: "high" }),
    makeTodo(session.id, 2, { content: "Clean the house", status: "done", priority: "medium" }),
    makeTodo(session.id, 3, { content: "Review pull request", status: "pending", priority: "low" }),
    makeTodo(session.id, 4, { content: "Buy birthday gift", status: "done", priority: "high" }),
    makeTodo(session.id, 5, { content: "Deploy to production", status: "pending", priority: "medium" }),
  ];
  await db.insert(TodoTable).values(todos);

  const sessionFilter = eq(TodoTable.session_id, session.id);

  // ILIKE basic pattern (case-insensitive search)
  results.push(
    await test("ILIKE basic pattern", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(and(sessionFilter, ilike(TodoTable.content, "%buy%")));
      // "Buy Groceries" and "Buy birthday gift" match case-insensitively
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // ILIKE with wildcard prefix
  results.push(
    await test("ILIKE wildcard prefix", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(and(sessionFilter, ilike(TodoTable.content, "%request")));
      // "Review pull request" matches
      if (rows.length !== 1)
        throw new Error(`Expected 1 row, got ${rows.length}`);
    })
  );

  // NOT ILIKE
  results.push(
    await test("NOT ILIKE", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(and(sessionFilter, notIlike(TodoTable.content, "%buy%")));
      // 5 - 2 = 3 rows that don't match %buy%
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows, got ${rows.length}`);
    })
  );

  // BETWEEN integer range
  results.push(
    await test("BETWEEN integer range", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(and(sessionFilter, between(TodoTable.position, 2, 4)));
      // positions 2, 3, 4
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows, got ${rows.length}`);
    })
  );

  // NOT BETWEEN
  results.push(
    await test("NOT BETWEEN", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(
          and(
            sessionFilter,
            not(between(TodoTable.position, 2, 4))
          )
        );
      // positions 1 and 5 are outside [2,4]
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // NOT single condition
  results.push(
    await test("NOT single condition", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(and(sessionFilter, not(eq(TodoTable.status, "done"))));
      // 3 pending, 2 done → 3 not done
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows, got ${rows.length}`);
    })
  );

  // NOT compound (NOT with OR inside)
  results.push(
    await test("NOT compound with OR", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(
          and(
            sessionFilter,
            not(
              or(
                eq(TodoTable.priority, "high"),
                eq(TodoTable.priority, "low")
              )
            )
          )
        );
      // Only "medium" priority: positions 2 and 5
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows, got ${rows.length}`);
    })
  );

  // Nested parenthesized WHERE
  results.push(
    await test("nested parenthesized WHERE", async () => {
      const rows = await db
        .select()
        .from(TodoTable)
        .where(
          and(
            sessionFilter,
            or(
              and(eq(TodoTable.status, "pending"), eq(TodoTable.priority, "high")),
              and(eq(TodoTable.status, "done"), eq(TodoTable.priority, "medium"))
            )
          )
        );
      // pending+high: position 1; done+medium: position 2
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows, got ${rows.length}`);
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
