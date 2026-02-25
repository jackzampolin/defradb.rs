// Realistic test data mimicking actual OpenCode session content.
// IDs use the same format OpenCode generates (nanoid-style).

let counter = 0;
function id(prefix: string): string {
  counter++;
  return `${prefix}_${counter.toString(36).padStart(8, "0")}`;
}

const now = Date.now();
function ts(offsetMs = 0): number {
  return now + offsetMs;
}

// --- Projects ---

export function makeProject(overrides: Record<string, unknown> = {}) {
  return {
    id: id("proj"),
    worktree: "/Users/jack/code/my-project",
    vcs: "git",
    name: "my-project",
    icon_url: null,
    icon_color: null,
    time_created: ts(),
    time_updated: ts(),
    time_initialized: ts(),
    sandboxes: JSON.stringify([]),
    commands: JSON.stringify({ start: "bun run dev" }),
    ...overrides,
  };
}

// --- Sessions ---

export function makeSession(
  projectId: string,
  overrides: Record<string, unknown> = {}
) {
  const sid = id("sess");
  return {
    id: sid,
    project_id: projectId,
    parent_id: null,
    slug: `session-${sid}`,
    directory: "/Users/jack/code/my-project",
    title: "Implement user authentication",
    version: "2",
    share_url: null,
    summary_additions: null,
    summary_deletions: null,
    summary_files: null,
    summary_diffs: null,
    revert: null,
    permission: null,
    time_created: ts(),
    time_updated: ts(),
    time_compacting: null,
    time_archived: null,
    ...overrides,
  };
}

// --- Messages ---

export function makeMessage(
  sessionId: string,
  role: "user" | "assistant" = "user",
  overrides: Record<string, unknown> = {}
) {
  return {
    id: id("msg"),
    session_id: sessionId,
    time_created: ts(counter * 1000),
    time_updated: ts(counter * 1000),
    data: JSON.stringify({
      role,
      tokens: { input: 150, output: 0, cache_read: 50, cache_write: 100 },
      cost: role === "assistant" ? 0.003 : 0,
      model: role === "assistant" ? "claude-sonnet-4-20250514" : undefined,
    }),
    ...overrides,
  };
}

// --- Parts ---

export function makeTextPart(
  messageId: string,
  sessionId: string,
  text: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    id: id("part"),
    message_id: messageId,
    session_id: sessionId,
    time_created: ts(counter * 1000),
    time_updated: ts(counter * 1000),
    data: JSON.stringify({ type: "text", text }),
    ...overrides,
  };
}

export function makeToolUsePart(
  messageId: string,
  sessionId: string,
  toolName: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    id: id("part"),
    message_id: messageId,
    session_id: sessionId,
    time_created: ts(counter * 1000),
    time_updated: ts(counter * 1000),
    data: JSON.stringify({
      type: "tool-invocation",
      toolInvocation: {
        toolName,
        state: "result",
        args: { path: "/src/auth.ts" },
        result: { success: true },
      },
    }),
    ...overrides,
  };
}

// --- Todos ---

export function makeTodo(
  sessionId: string,
  position: number,
  overrides: Record<string, unknown> = {}
) {
  return {
    session_id: sessionId,
    content: `Task item ${position}`,
    status: "pending",
    priority: "medium",
    position,
    time_created: ts(),
    time_updated: ts(),
    ...overrides,
  };
}

// --- Permissions ---

export function makePermission(
  projectId: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    project_id: projectId,
    time_created: ts(),
    time_updated: ts(),
    data: JSON.stringify({
      version: "1",
      rules: [
        { tool: "Bash", allow: true },
        { tool: "Read", allow: true },
      ],
    }),
    ...overrides,
  };
}

// --- SessionShare ---

export function makeSessionShare(
  sessionId: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    session_id: sessionId,
    id: id("share"),
    secret: `sec_${Math.random().toString(36).slice(2)}`,
    url: `https://share.opencode.ai/s/${id("pub")}`,
    time_created: ts(),
    time_updated: ts(),
    ...overrides,
  };
}

// Bulk generators for pagination tests
export function makeSessions(projectId: string, count: number) {
  return Array.from({ length: count }, (_, i) =>
    makeSession(projectId, {
      title: `Session ${i + 1}`,
      time_updated: ts(i * 60_000),
    })
  );
}
