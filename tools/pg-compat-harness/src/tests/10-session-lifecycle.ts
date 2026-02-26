// 10-session-lifecycle: Full OpenCode session lifecycle.
// create project → create session → add messages → add parts → fork → delete

import { db } from "../db";
import { eq, desc } from "drizzle-orm";
import {
  ProjectTable,
  SessionTable,
  MessageTable,
  PartTable,
  TodoTable,
  PermissionTable,
  SessionShareTable,
} from "../schema";
import {
  makeProject,
  makeSession,
  makeMessage,
  makeTextPart,
  makeToolUsePart,
  makeTodo,
  makePermission,
  makeSessionShare,
} from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  // Step 1: Initialize project
  results.push(
    await test("lifecycle: create project", async () => {
      const proj = makeProject({
        id: "proj_lifecycle",
        name: "lifecycle-test",
        worktree: "/Users/jack/code/lifecycle-test",
      });
      await db.insert(ProjectTable).values(proj);
      await db.insert(PermissionTable).values(
        makePermission("proj_lifecycle")
      );

      const rows = await db
        .select()
        .from(ProjectTable)
        .where(eq(ProjectTable.id, "proj_lifecycle"));
      if (rows.length !== 1) throw new Error("Project not created");
    })
  );

  // Step 2: Start session
  results.push(
    await test("lifecycle: start session", async () => {
      const sess = makeSession("proj_lifecycle", {
        id: "sess_lifecycle_1",
        title: "Implement auth flow",
        slug: "implement-auth-flow",
      });
      await db.insert(SessionTable).values(sess);
    })
  );

  // Step 3: User sends message with text part
  results.push(
    await test("lifecycle: user message", async () => {
      const msg = makeMessage("sess_lifecycle_1", "user", {
        id: "msg_user_1",
      });
      await db.insert(MessageTable).values(msg);

      const part = makeTextPart(
        "msg_user_1",
        "sess_lifecycle_1",
        "Add JWT authentication to the Express API",
        { id: "part_user_text_1" }
      );
      await db.insert(PartTable).values(part);
    })
  );

  // Step 4: Assistant responds with text + tool use parts
  results.push(
    await test("lifecycle: assistant response with tool use", async () => {
      const msg = makeMessage("sess_lifecycle_1", "assistant", {
        id: "msg_asst_1",
      });
      await db.insert(MessageTable).values(msg);

      const textPart = makeTextPart(
        "msg_asst_1",
        "sess_lifecycle_1",
        "I'll add JWT authentication. Let me read the existing code first.",
        { id: "part_asst_text_1" }
      );
      const toolPart = makeToolUsePart(
        "msg_asst_1",
        "sess_lifecycle_1",
        "Read",
        { id: "part_asst_tool_1" }
      );
      await db.insert(PartTable).values([textPart, toolPart]);

      // Verify parts ordered by message
      const parts = await db
        .select()
        .from(PartTable)
        .where(eq(PartTable.message_id, "msg_asst_1"));
      if (parts.length !== 2) throw new Error(`Expected 2 parts, got ${parts.length}`);
    })
  );

  // Step 5: Add todos
  results.push(
    await test("lifecycle: add session todos", async () => {
      const todos = [
        makeTodo("sess_lifecycle_1", 0, {
          content: "Add jwt dependency",
          status: "completed",
        }),
        makeTodo("sess_lifecycle_1", 1, {
          content: "Create auth middleware",
          status: "in_progress",
        }),
        makeTodo("sess_lifecycle_1", 2, {
          content: "Add route protection",
          status: "pending",
        }),
      ];
      await db.insert(TodoTable).values(todos);
    })
  );

  // Step 6: Update session summary
  results.push(
    await test("lifecycle: update session summary", async () => {
      await db
        .update(SessionTable)
        .set({
          summary_additions: 87,
          summary_deletions: 3,
          summary_files: 4,
          summary_diffs: JSON.stringify([
            { path: "src/auth.ts", additions: 45, deletions: 0 },
            { path: "src/middleware.ts", additions: 30, deletions: 3 },
            { path: "package.json", additions: 2, deletions: 0 },
            { path: "src/routes.ts", additions: 10, deletions: 0 },
          ]),
          time_updated: Date.now(),
        })
        .where(eq(SessionTable.id, "sess_lifecycle_1"));
    })
  );

  // Step 7: Fork session (create child with parent_id)
  results.push(
    await test("lifecycle: fork session", async () => {
      const forked = makeSession("proj_lifecycle", {
        id: "sess_lifecycle_fork",
        parent_id: "sess_lifecycle_1",
        title: "Implement auth flow (fork — try OAuth instead)",
        slug: "implement-auth-fork",
      });
      await db.insert(SessionTable).values(forked);

      // Query sessions by parent
      const children = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.parent_id, "sess_lifecycle_1"));
      if (children.length !== 1) throw new Error("Fork not found");
      if (children[0].id !== "sess_lifecycle_fork")
        throw new Error("Wrong fork id");
    })
  );

  // Step 8: Share session
  results.push(
    await test("lifecycle: share session", async () => {
      const share = makeSessionShare("sess_lifecycle_1", {
        id: "share_lifecycle_1",
        url: "https://share.opencode.ai/s/lifecycle_1",
      });
      await db.insert(SessionShareTable).values(share);

      // Update session share_url
      await db
        .update(SessionTable)
        .set({ share_url: "https://share.opencode.ai/s/lifecycle_1" })
        .where(eq(SessionTable.id, "sess_lifecycle_1"));
    })
  );

  // Step 9: List sessions for project (the common dashboard query)
  results.push(
    await test("lifecycle: list project sessions", async () => {
      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, "proj_lifecycle"))
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id));
      if (sessions.length !== 2) throw new Error(`Expected 2 sessions, got ${sessions.length}`);
    })
  );

  // Step 10: Delete forked session (cascade should clean its children)
  results.push(
    await test("lifecycle: delete fork", async () => {
      await db
        .delete(SessionTable)
        .where(eq(SessionTable.id, "sess_lifecycle_fork"));
      const remaining = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, "proj_lifecycle"));
      if (remaining.length !== 1) throw new Error("Fork delete failed");
    })
  );

  // Step 11: Delete the whole project (cascades everything)
  results.push(
    await test("lifecycle: delete project cascades all", async () => {
      await db
        .delete(ProjectTable)
        .where(eq(ProjectTable.id, "proj_lifecycle"));

      // Everything should be gone
      const sessions = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.project_id, "proj_lifecycle"));
      if (sessions.length !== 0) throw new Error("Sessions not cascaded");

      const perms = await db
        .select()
        .from(PermissionTable)
        .where(eq(PermissionTable.project_id, "proj_lifecycle"));
      if (perms.length !== 0) throw new Error("Permissions not cascaded");
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
