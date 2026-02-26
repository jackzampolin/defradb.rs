import { drizzle } from "drizzle-orm/postgres-js";
import { DefaultLogger } from "drizzle-orm/logger";
import postgres from "postgres";
import * as schema from "./schema";

const url = process.env.PG_URL;
if (!url) {
  console.error("PG_URL environment variable is required");
  process.exit(1);
}

const verbose = process.env.VERBOSE === "1";

export const sql = postgres(url, {
  max: 5,
  idle_timeout: 10,
  connect_timeout: 10,
});

export const db = drizzle(sql, {
  schema,
  logger: verbose ? new DefaultLogger() : false,
});
