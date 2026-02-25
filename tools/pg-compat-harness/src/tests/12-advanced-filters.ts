// 12-advanced-filters: WHERE clause patterns beyond basic eq/and/in/like.
// Tests OR, IS NULL, IS NOT NULL, comparison operators, NOT IN, NOT LIKE.

import { db } from "../db";
import {
  eq,
  and,
  or,
  not,
  gt,
  gte,
  lt,
  lte,
  ne,
  isNull,
  isNotNull,
  notInArray,
  notLike,
  between,
  desc,
} from "drizzle-orm";
import { ProjectTable, SessionTable, TodoTable } from "../schema";
import { makeProject, makeSession, makeTodo } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Seed data: 1 project, 5 sessions with varying attributes
  const proj = makeProject({ name: "filter-test-project" });
  await db.insert(ProjectTable).values(proj);

  const sessions = [
    makeSession(proj.id, {
      title: "Alpha",
      time_updated: 1000,
      parent_id: null,
      share_url: null,
      summary_additions: 10,
    }),
    makeSession(proj.id, {
      title: "Beta",
      time_updated: 2000,
      parent_id: null,
      share_url: "https://share.example.com/beta",
      summary_additions: 20,
    }),
    makeSession(proj.id, {
      title: "Gamma",
      time_updated: 3000,
      parent_id: null,
      share_url: null,
      summary_additions: 30,
    }),
    makeSession(proj.id, {
      title: "Delta",
      time_updated: 4000,
      parent_id: null,
      share_url: "https://share.example.com/delta",
      summary_additions: 40,
    }),
    makeSession(proj.id, {
      title: "Epsilon",
      time_updated: 5000,
      parent_id: null,
      share_url: null,
      summary_additions: 50,
    }),
  ];
  // Set parent_id on some to create relationships
  sessions[2] = { ...sessions[2], parent_id: sessions[0].id };
  sessions[4] = { ...sessions[4], parent_id: sessions[1].id };
  await db.insert(SessionTable).values(sessions);

  // OR filter
  results.push(
    await test("select with OR", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            or(
              eq(SessionTable.title, "Alpha"),
              eq(SessionTable.title, "Gamma")
            )
          )
        );
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows, got ${rows.length}`);
      const titles = rows.map((r) => r.title).sort();
      if (titles[0] !== "Alpha" || titles[1] !== "Gamma")
        throw new Error(`Wrong titles: ${JSON.stringify(titles)}`);
    })
  );

  // IS NULL
  results.push(
    await test("select with IS NULL", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            isNull(SessionTable.share_url)
          )
        );
      // Alpha, Gamma, Epsilon have null share_url
      if (rows.length !== 3)
        throw new Error(`Expected 3 null share_url rows, got ${rows.length}`);
    })
  );

  // IS NOT NULL
  results.push(
    await test("select with IS NOT NULL", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            isNotNull(SessionTable.share_url)
          )
        );
      // Beta, Delta have non-null share_url
      if (rows.length !== 2)
        throw new Error(`Expected 2 non-null share_url rows, got ${rows.length}`);
    })
  );

  // Greater than (>)
  results.push(
    await test("select with GT", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            gt(SessionTable.summary_additions, 30)
          )
        );
      // Delta (40) and Epsilon (50)
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows with additions > 30, got ${rows.length}`);
    })
  );

  // Greater than or equal (>=)
  results.push(
    await test("select with GTE", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            gte(SessionTable.summary_additions, 30)
          )
        );
      // Gamma (30), Delta (40), Epsilon (50)
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows with additions >= 30, got ${rows.length}`);
    })
  );

  // Less than (<)
  results.push(
    await test("select with LT", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            lt(SessionTable.summary_additions, 30)
          )
        );
      // Alpha (10), Beta (20)
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows with additions < 30, got ${rows.length}`);
    })
  );

  // Less than or equal (<=)
  results.push(
    await test("select with LTE", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            lte(SessionTable.summary_additions, 30)
          )
        );
      // Alpha (10), Beta (20), Gamma (30)
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows with additions <= 30, got ${rows.length}`);
    })
  );

  // Not equal (!=)
  results.push(
    await test("select with NE", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            ne(SessionTable.title, "Alpha")
          )
        );
      if (rows.length !== 4)
        throw new Error(`Expected 4 rows != Alpha, got ${rows.length}`);
    })
  );

  // NOT IN
  results.push(
    await test("select with NOT IN", async () => {
      const excluded = [sessions[0].id, sessions[1].id];
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            notInArray(SessionTable.id, excluded)
          )
        );
      // Gamma, Delta, Epsilon
      if (rows.length !== 3)
        throw new Error(`Expected 3 rows not in excluded, got ${rows.length}`);
    })
  );

  // NOT LIKE
  results.push(
    await test("select with NOT LIKE", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            notLike(SessionTable.title, "%a%")
          )
        );
      // Only "Epsilon" has no lowercase 'a' (Alpha, Beta, Gamma, Delta all have 'a')
      if (rows.length !== 1)
        throw new Error(`Expected 1 row not matching %a%, got ${rows.length}`);
      if (rows[0].title !== "Epsilon")
        throw new Error(`Expected Epsilon, got ${rows[0].title}`);
    })
  );

  // Combined: OR with comparison operators
  results.push(
    await test("combined OR with comparisons", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            or(
              lt(SessionTable.summary_additions, 15),
              gt(SessionTable.summary_additions, 45)
            )
          )
        );
      // Alpha (10) and Epsilon (50)
      if (rows.length !== 2)
        throw new Error(`Expected 2 rows (< 15 or > 45), got ${rows.length}`);
    })
  );

  // Combined: IS NULL OR specific value
  results.push(
    await test("OR with IS NULL and eq", async () => {
      const rows = await db
        .select()
        .from(SessionTable)
        .where(
          and(
            eq(SessionTable.project_id, proj.id),
            or(
              isNull(SessionTable.parent_id),
              eq(SessionTable.title, "Gamma")
            )
          )
        );
      // Alpha, Beta, Delta have null parent_id; Gamma matches by title (and also has parent)
      // Epsilon has parent_id set
      // So: Alpha(null parent), Beta(null parent), Gamma(matches title), Delta(null parent), Epsilon(has parent but Gamma already counted)
      // Wait - Gamma has parent_id set to sessions[0].id. So null parent_id: Alpha, Beta, Delta = 3
      // OR title = Gamma: Gamma = 1
      // Union: 4
      if (rows.length !== 4)
        throw new Error(`Expected 4 rows, got ${rows.length}`);
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
