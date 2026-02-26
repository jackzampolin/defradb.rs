// CLI runner: connect to PG_URL, run all test categories, report pass/fail.
// Usage: PG_URL=postgres://... bun run src/run.ts
//   or:  bun run src/run.ts --embedded   (spins up embedded PG, runs, tears down)

export interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
}

interface CategoryResult {
  category: string;
  results: TestResult[];
  passed: boolean;
  duration: number;
}

import * as t01 from "./tests/01-connection";
import * as t02 from "./tests/02-ddl";
import * as t03 from "./tests/03-insert";
import * as t04 from "./tests/04-select";
import * as t05 from "./tests/05-update";
import * as t06 from "./tests/06-delete";
import * as t07 from "./tests/07-transactions";
import * as t08 from "./tests/08-pagination";
import * as t09 from "./tests/09-json";
import * as t10 from "./tests/10-session-lifecycle";
import * as t11 from "./tests/11-txn-read-your-writes";
import * as t12 from "./tests/12-advanced-filters";
import * as t13 from "./tests/13-edge-cases";
import * as t14 from "./tests/14-aggregates";
import * as t15 from "./tests/15-group-by";
import * as t16 from "./tests/16-joins";
import * as t17 from "./tests/17-distinct";
import * as t18 from "./tests/18-subqueries";
import * as t19 from "./tests/19-set-operations";

const categories: { name: string; run: () => Promise<TestResult[]> }[] = [
  { name: "01-connection", run: t01.run },
  { name: "02-ddl", run: t02.run },
  { name: "03-insert", run: t03.run },
  { name: "04-select", run: t04.run },
  { name: "05-update", run: t05.run },
  { name: "06-delete", run: t06.run },
  { name: "07-transactions", run: t07.run },
  { name: "08-pagination", run: t08.run },
  { name: "09-json", run: t09.run },
  { name: "10-session-lifecycle", run: t10.run },
  { name: "11-txn-read-your-writes", run: t11.run },
  { name: "12-advanced-filters", run: t12.run },
  { name: "13-edge-cases", run: t13.run },
  { name: "14-aggregates", run: t14.run },
  { name: "15-group-by", run: t15.run },
  { name: "16-joins", run: t16.run },
  { name: "17-distinct", run: t17.run },
  { name: "18-subqueries", run: t18.run },
  { name: "19-set-operations", run: t19.run },
];

async function main() {
  const url = process.env.PG_URL;
  if (!url) {
    console.error("PG_URL is required. Set it or use: bun run src/embedded-test.ts");
    process.exit(1);
  }

  console.log(`\nTarget: ${url.replace(/:[^:@]+@/, ":***@")}`);
  console.log("=".repeat(60));

  const categoryResults: CategoryResult[] = [];
  let totalPassed = 0;
  let totalFailed = 0;

  for (const cat of categories) {
    const start = performance.now();
    let results: TestResult[];
    try {
      results = await cat.run();
    } catch (e: any) {
      results = [{ name: "category setup", passed: false, error: e.message }];
    }
    const duration = performance.now() - start;

    const passed = results.every((r) => r.passed);
    categoryResults.push({ category: cat.name, results, passed, duration });

    const icon = passed ? "PASS" : "FAIL";
    const passCount = results.filter((r) => r.passed).length;
    const failCount = results.filter((r) => !r.passed).length;
    totalPassed += passCount;
    totalFailed += failCount;

    console.log(
      `\n[${icon}] ${cat.name}  (${passCount}/${results.length} tests, ${duration.toFixed(0)}ms)`
    );

    for (const r of results) {
      if (r.passed) {
        console.log(`  + ${r.name}`);
      } else {
        console.log(`  - ${r.name}: ${r.error}`);
      }
    }
  }

  // Summary
  console.log("\n" + "=".repeat(60));
  console.log("SUMMARY");
  console.log("=".repeat(60));

  for (const cr of categoryResults) {
    const icon = cr.passed ? "PASS" : "FAIL";
    console.log(`  [${icon}] ${cr.category}`);
  }

  const totalTests = totalPassed + totalFailed;
  const allPassed = totalFailed === 0;
  console.log(
    `\n${totalPassed}/${totalTests} tests passed across ${categoryResults.length} categories`
  );

  // Write JSON results to results/ if it exists
  const resultsDir = new URL("../results/", import.meta.url).pathname;
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const outPath = `${resultsDir}run-${timestamp}.json`;
    await Bun.write(
      outPath,
      JSON.stringify({ timestamp: new Date().toISOString(), url: url.replace(/:[^:@]+@/, ":***@"), categories: categoryResults }, null, 2)
    );
    console.log(`Results written to ${outPath}`);
  } catch {
    // results/ may not exist, that's fine
  }

  // Close connection pool
  const { sql } = await import("./db");
  await sql.end();

  process.exit(allPassed ? 0 : 1);
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(2);
});
