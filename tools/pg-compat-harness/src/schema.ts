// PG translation of OpenCode's Drizzle schema (originally SQLite).
// Source: anomalyco/opencode packages/opencode/src/{session,project,share,control}/*.sql.ts
//
// Translation rules:
//   SQLite text()         → PG text()
//   SQLite integer()      → PG bigint() (timestamps are ms-epoch)
//   SQLite text({ mode: "json" }) → PG text() (OpenCode stores JSON as text, parses in app)
//   SQLite integer({ mode: "boolean" }) → PG boolean()

import {
  pgTable,
  text,
  bigint,
  boolean,
  index,
  primaryKey,
} from "drizzle-orm/pg-core";

// --- Project ---

export const ProjectTable = pgTable("project", {
  id: text("id").primaryKey(),
  worktree: text("worktree").notNull(),
  vcs: text("vcs"),
  name: text("name"),
  icon_url: text("icon_url"),
  icon_color: text("icon_color"),
  time_created: bigint("time_created", { mode: "number" }).notNull(),
  time_updated: bigint("time_updated", { mode: "number" }).notNull(),
  time_initialized: bigint("time_initialized", { mode: "number" }),
  sandboxes: text("sandboxes").notNull(), // JSON array as text
  commands: text("commands"), // JSON object as text
});

// --- Session ---

export const SessionTable = pgTable(
  "session",
  {
    id: text("id").primaryKey(),
    project_id: text("project_id")
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    parent_id: text("parent_id"),
    slug: text("slug").notNull(),
    directory: text("directory").notNull(),
    title: text("title").notNull(),
    version: text("version").notNull(),
    share_url: text("share_url"),
    summary_additions: bigint("summary_additions", { mode: "number" }),
    summary_deletions: bigint("summary_deletions", { mode: "number" }),
    summary_files: bigint("summary_files", { mode: "number" }),
    summary_diffs: text("summary_diffs"), // JSON as text
    revert: text("revert"), // JSON as text
    permission: text("permission"), // JSON as text
    time_created: bigint("time_created", { mode: "number" }).notNull(),
    time_updated: bigint("time_updated", { mode: "number" }).notNull(),
    time_compacting: bigint("time_compacting", { mode: "number" }),
    time_archived: bigint("time_archived", { mode: "number" }),
  },
  (table) => [
    index("session_project_idx").on(table.project_id),
    index("session_parent_idx").on(table.parent_id),
  ]
);

// --- Message ---

export const MessageTable = pgTable(
  "message",
  {
    id: text("id").primaryKey(),
    session_id: text("session_id")
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    time_created: bigint("time_created", { mode: "number" }).notNull(),
    time_updated: bigint("time_updated", { mode: "number" }).notNull(),
    data: text("data").notNull(), // JSON as text
  },
  (table) => [index("message_session_idx").on(table.session_id)]
);

// --- Part ---

export const PartTable = pgTable(
  "part",
  {
    id: text("id").primaryKey(),
    message_id: text("message_id")
      .notNull()
      .references(() => MessageTable.id, { onDelete: "cascade" }),
    session_id: text("session_id").notNull(),
    time_created: bigint("time_created", { mode: "number" }).notNull(),
    time_updated: bigint("time_updated", { mode: "number" }).notNull(),
    data: text("data").notNull(), // JSON as text
  },
  (table) => [
    index("part_message_idx").on(table.message_id),
    index("part_session_idx").on(table.session_id),
  ]
);

// --- Todo ---

export const TodoTable = pgTable(
  "todo",
  {
    session_id: text("session_id")
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    content: text("content").notNull(),
    status: text("status").notNull(),
    priority: text("priority").notNull(),
    position: bigint("position", { mode: "number" }).notNull(),
    time_created: bigint("time_created", { mode: "number" }).notNull(),
    time_updated: bigint("time_updated", { mode: "number" }).notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.position] }),
    index("todo_session_idx").on(table.session_id),
  ]
);

// --- Permission ---

export const PermissionTable = pgTable("permission", {
  project_id: text("project_id")
    .primaryKey()
    .references(() => ProjectTable.id, { onDelete: "cascade" }),
  time_created: bigint("time_created", { mode: "number" }).notNull(),
  time_updated: bigint("time_updated", { mode: "number" }).notNull(),
  data: text("data").notNull(), // JSON as text
});

// --- SessionShare ---

export const SessionShareTable = pgTable("session_share", {
  session_id: text("session_id")
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  id: text("id").notNull(),
  secret: text("secret").notNull(),
  url: text("url").notNull(),
  time_created: bigint("time_created", { mode: "number" }).notNull(),
  time_updated: bigint("time_updated", { mode: "number" }).notNull(),
});
