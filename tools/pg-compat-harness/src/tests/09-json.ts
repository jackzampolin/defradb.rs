// 09-json: JSON column read/write (text mode with JSON.parse in app).
// OpenCode stores structured data as JSON text in `data` columns.

import { db } from "../db";
import { eq } from "drizzle-orm";
import { ProjectTable, SessionTable, MessageTable, PartTable, PermissionTable } from "../schema";
import { makeProject, makeSession, makeMessage } from "../fixtures";
import type { TestResult } from "../run";

export async function run(): Promise<TestResult[]> {
  const results: TestResult[] = [];

  const proj = makeProject({ name: "json-test" });
  await db.insert(ProjectTable).values(proj);
  const sess = makeSession(proj.id);
  await db.insert(SessionTable).values(sess);

  // Message with complex JSON data column
  results.push(
    await test("insert + read JSON data (message)", async () => {
      const data = {
        role: "assistant",
        tokens: { input: 1500, output: 3200, cache_read: 800, cache_write: 700 },
        cost: 0.0156,
        model: "claude-sonnet-4-20250514",
        usage: {
          inputTokens: 1500,
          outputTokens: 3200,
          cacheCreationInputTokens: 700,
          cacheReadInputTokens: 800,
        },
      };
      const msg = makeMessage(sess.id, "assistant", {
        data: JSON.stringify(data),
      });
      await db.insert(MessageTable).values(msg);

      const rows = await db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.id, msg.id));
      const parsed = JSON.parse(rows[0].data);
      if (parsed.role !== "assistant") throw new Error("role mismatch");
      if (parsed.tokens.input !== 1500) throw new Error("tokens.input mismatch");
      if (parsed.cost !== 0.0156) throw new Error("cost mismatch");
    })
  );

  // Part with tool-invocation JSON
  results.push(
    await test("insert + read JSON data (part — tool invocation)", async () => {
      const msg = makeMessage(sess.id, "assistant");
      await db.insert(MessageTable).values(msg);

      const data = {
        type: "tool-invocation",
        toolInvocation: {
          toolName: "Read",
          state: "result",
          args: { file_path: "/src/main.ts", offset: 0, limit: 100 },
          result: {
            content: 'import express from "express";\n\nconst app = express();',
            lineCount: 3,
          },
        },
      };
      const part = {
        id: `part_json_tool_${Date.now()}`,
        message_id: msg.id,
        session_id: sess.id,
        time_created: Date.now(),
        time_updated: Date.now(),
        data: JSON.stringify(data),
      };
      await db.insert(PartTable).values(part);

      const rows = await db
        .select()
        .from(PartTable)
        .where(eq(PartTable.id, part.id));
      const parsed = JSON.parse(rows[0].data);
      if (parsed.type !== "tool-invocation") throw new Error("type mismatch");
      if (parsed.toolInvocation.toolName !== "Read")
        throw new Error("toolName mismatch");
    })
  );

  // Session with JSON summary_diffs
  results.push(
    await test("insert + read JSON (session summary_diffs)", async () => {
      const diffs = [
        { path: "src/auth.ts", additions: 45, deletions: 12 },
        { path: "src/middleware.ts", additions: 8, deletions: 0 },
      ];
      await db
        .update(SessionTable)
        .set({ summary_diffs: JSON.stringify(diffs) })
        .where(eq(SessionTable.id, sess.id));

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      const parsed = JSON.parse(rows[0].summary_diffs!);
      if (parsed.length !== 2) throw new Error("diffs array length wrong");
      if (parsed[0].path !== "src/auth.ts") throw new Error("path mismatch");
    })
  );

  // Permission with JSON ruleset
  results.push(
    await test("insert + read JSON (permission ruleset)", async () => {
      const ruleset = {
        version: "1",
        rules: [
          { tool: "Bash", allow: true, pattern: "npm *" },
          { tool: "Write", allow: false, pattern: "*.env" },
          { tool: "Read", allow: true },
        ],
      };
      await db.insert(PermissionTable).values({
        project_id: proj.id,
        time_created: Date.now(),
        time_updated: Date.now(),
        data: JSON.stringify(ruleset),
      });

      const rows = await db
        .select()
        .from(PermissionTable)
        .where(eq(PermissionTable.project_id, proj.id));
      const parsed = JSON.parse(rows[0].data);
      if (parsed.rules.length !== 3) throw new Error("rules count wrong");
      if (parsed.rules[1].pattern !== "*.env") throw new Error("pattern mismatch");
    })
  );

  // Session with JSON revert field
  results.push(
    await test("insert + read JSON (session revert)", async () => {
      const revert = {
        messageID: "msg_abc123",
        partID: "part_def456",
        snapshot: "snapshot_v1",
        diff: "--- a/src/main.ts\n+++ b/src/main.ts\n@@ -1 +1 @@\n-old\n+new",
      };
      await db
        .update(SessionTable)
        .set({ revert: JSON.stringify(revert) })
        .where(eq(SessionTable.id, sess.id));

      const rows = await db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, sess.id));
      const parsed = JSON.parse(rows[0].revert!);
      if (parsed.messageID !== "msg_abc123") throw new Error("messageID mismatch");
      if (!parsed.diff.includes("--- a/src/main.ts"))
        throw new Error("diff content mismatch");
    })
  );

  // Cleanup
  await db.delete(SessionTable).where(eq(SessionTable.id, sess.id));
  await db.delete(ProjectTable).where(eq(ProjectTable.id, proj.id));

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
