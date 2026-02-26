import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/schema.ts",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.PG_URL || "postgres://postgres:test@localhost:5432/postgres",
  },
});
