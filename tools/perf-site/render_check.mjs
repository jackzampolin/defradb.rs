// Render the dashboard headlessly and refuse a publish that would drop a
// family on the floor.
//
// The page reads run documents the benches produce. Nothing links the two, so
// a bench can change the shape of what it records and the section that draws
// it silently renders nothing: a blank section is not an error to a browser,
// so the job stays green and the site goes quiet.
//
// This runs the page's own code over the documents about to be published and
// fails when a family that was collected produced no section, so the failure
// lands in CI rather than on the site.
//
// Usage: node tools/perf-site/render_check.mjs <site dir>

import { readFileSync } from "node:fs";

const site = process.argv[2];
if (!site) {
  console.error("usage: render_check.mjs <site dir>");
  process.exit(2);
}

const html = readFileSync(`${site}/index.html`, "utf8");
const script = html.slice(html.indexOf("<script>") + 8, html.lastIndexOf("</script>"));

// Enough of a <select> for the page's own boot path to run unchanged: a
// browser adopts the selected option's value the moment the options are
// written, and `boot` depends on that to pick the run and the platform it
// draws. Setting the values from here instead would test a path the browser
// never takes.
let nodes = {};
const node = (id) =>
  (nodes[id] ??= {
    _html: "",
    // The static markup marks 10% selected; the stub starts where the browser
    // would, so a threshold-dependent class is computed against a real number.
    value: id === "#thresh" ? "10" : "",
    style: {},
    addEventListener() {},
    set innerHTML(v) {
      this._html = v;
      const opts = [...v.matchAll(/<option value="([^"]*)"([^>]*)>/g)];
      if (opts.length) this.value = (opts.find((o) => /\bselected\b/.test(o[2])) || opts[0])[1];
    },
    get innerHTML() {
      return this._html;
    },
  });

// The page also wires up interaction it cannot use here: a zoom overlay it
// builds and appends, and document-level listeners. None of it affects what
// gets rendered, but it runs during boot, so it has to find something rather
// than throw and take the whole check down with it. These stubs exist to let
// boot reach the end, not to stand in for a browser: nothing below is
// asserted on.
const detached = () => ({
  className: "",
  hidden: false,
  style: {},
  classList: { add() {}, remove() {}, contains: () => false },
  appendChild() {},
  addEventListener() {},
  closest: () => null,
});

globalThis.document = {
  querySelector: (sel) => node(sel),
  querySelectorAll: () => [],
  createElement: () => detached(),
  addEventListener() {},
  body: { appendChild() {} },
};
// The page sizes its charts from the viewport and redraws when it changes, so
// the check has to stand in for a window as well as a document. A fixed width
// keeps the rendered output deterministic.
globalThis.window = {
  innerWidth: 1280,
  innerHeight: 900,
  addEventListener() {},
  matchMedia: () => ({ matches: false, addEventListener() {} }),
};

globalThis.fetch = async (path) => {
  try {
    return { ok: true, json: async () => JSON.parse(readFileSync(`${site}/${path}`, "utf8")) };
  } catch {
    return { ok: false, status: 404 };
  }
};

let index;
try {
  index = JSON.parse(readFileSync(`${site}/runs/index.json`, "utf8"));
} catch {
  console.error("render check: no runs/index.json, nothing to verify");
  process.exit(1);
}
if (!index.runs?.length) {
  console.error("render check: runs/index.json lists no runs");
  process.exit(1);
}

/// Render the page once, for one platform.
///
/// A run holds a platform per runner and the page draws one at a time, so
/// checking only whichever the picker landed on leaves every other platform
/// unverified. That is exactly the gap this check exists to close, so each one
/// gets its own render against fresh stubs.
async function render(platform) {
  nodes = {};
  // Seeded before boot because `syncPlatforms` reads the current value first
  // and restores it when the new run also has that platform, which is the same
  // path a browser takes when the reader changes runs.
  if (platform) node("#plat").value = platform;
  new Function(script)();
  await new Promise((r) => setTimeout(r, 250));
  return { html: node("#main").innerHTML, platform: node("#plat").value };
}

const failures = [];
const newestRun = JSON.parse(readFileSync(`${site}/runs/${index.runs[0].file}`, "utf8"));
const platforms = Object.keys(newestRun.platforms ?? {});
if (!platforms.length) {
  console.error("render check: the newest run records no platform");
  process.exit(1);
}

let out = "";
let checked = 0;
let cardsTotal = 0;
for (const wanted of platforms) {
  const drawn = await render(wanted);
  if (drawn.platform !== wanted) {
    failures.push(`the page would not select ${wanted}; it drew ${drawn.platform || "nothing"}`);
    continue;
  }
  out = drawn.html;
  cardsTotal += (out.match(/<h3>/g) || []).length;
  checked += 1;
  verify(out, wanted, newestRun);
}
out = out || "";

function verify(out, platform, newest) {
// A section that throws is not a section that is missing, and until this
// existed it was not a failure either: `section(name, build)` catches every
// throw and emits the heading plus a `card broken` panel, which satisfies a
// naive heading assertion. Look for the panel first, and name it.
for (const m of out.matchAll(
  /<div class="card broken">\s*<h3>([^<]*)<\/h3>\s*<p class="note">([^<]*)<\/p>/g
)) {
  failures.push(`${platform}: ${m[1]}: ${m[2]}`);
}
if (/class="card broken"/.test(out) && !failures.some((f) => f.startsWith(`${platform}:`))) {
  failures.push(`${platform}: a section rendered as a broken card, in a shape this check could not name`);
}
// Nothing below may throw on a malformed document. A check that crashes
// reports "the check broke", and the reader has to go and find out which
// family did it; a check that keeps going names the family in its own output.
const asArray = (v) => (Array.isArray(v) ? v : []);
const families = newest.platforms?.[platform]?.families ?? {};
let drawn = 0;
for (const [name, family] of Object.entries(families)) {
  if (!family || family.trust === "absent") continue;
  const title = family.title || name;
  // The heading carries a trust pill after the title, so match the opening.
  if (!out.includes(`<span class="h2-name">${title}</span>`)) {
    failures.push(`${platform}: ${name} was collected (trust=${family.trust}) but "${title}" is not on the page`);
    continue;
  }
  drawn += 1;

  // A family that claims to be collected owes the page a group. The renderer
  // degrades a malformed `groups` into an empty list and draws a card saying
  // so, which is honest on the page but must still fail the publish: the run
  // document is the defect, not the page.
  if (!Array.isArray(family.groups)) {
    failures.push(
      `${platform}: ${name} was collected (trust=${family.trust}) but its "groups" is ` +
        `${family.groups === undefined ? "missing" : typeof family.groups}, not an array`
    );
    continue;
  }
  if (!family.groups.length) {
    failures.push(`${platform}: ${name} was collected (trust=${family.trust}) but recorded no groups`);
    continue;
  }
  for (const group of family.groups) {
    const rows = asArray(group?.rows).filter((r) => Number.isFinite(r?.value)).length;
    if (!rows) {
      failures.push(`${platform}: ${name}/${group?.name} recorded no usable row`);
      continue;
    }
    if (!out.includes(`<h3>${group.name}`)) {
      failures.push(`${platform}: ${name}/${group.name} has ${rows} row(s) but drew no card`);
    }
  }
}
if (!drawn) {
  failures.push(
    `no family on ${platform} was drawn; the newest run lists ${Object.keys(families).length}`
  );
}
const drewHeading = (name) => out.includes(`<span class="h2-name">${name}</span>`);
if (!drewHeading("Overview")) failures.push(`${platform}: the overview section is missing`);
if (Object.keys(newest.platforms ?? {}).length > 1 && !drewHeading("Across platforms")) {
  failures.push(`${platform}: the run has more than one platform but the cross-platform section is missing`);
}
if (index.runs.length && !drewHeading("Trend across every recorded run")) {
  failures.push(`${platform}: the trend section is missing`);
}
if (out.length < 2000) {
  failures.push(`${platform}: the page rendered only ${out.length} bytes, which is not a populated dashboard`);
}

// The jump index is built by reading back the headings the page produced, so a
// link that points at no heading means the two drifted. A dead anchor is silent
// in a browser: the click simply does nothing.
const headings = new Set([...out.matchAll(/<h2 id="([^"]+)"/g)].map((m) => m[1]));
for (const m of out.matchAll(/<a class="jump" href="#([^"]+)"/g)) {
  if (!headings.has(m[1])) {
    failures.push(`${platform}: the index links to #${m[1]}, which is not a heading on the page`);
  }
}
}

console.log(
  `render check: ${checked} of ${platforms.length} platform(s) verified ` +
    `(${platforms.join(", ")}), ${cardsTotal} cards across them, ` +
    `${index.runs.length} run(s) on file`
);

if (failures.length) {
  for (const f of failures) console.error(`render check: ${f}`);
  process.exit(1);
}
