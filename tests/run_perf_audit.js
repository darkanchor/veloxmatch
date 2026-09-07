#!/usr/bin/env bun
// Paired native fixtures: sequential runs, with every result/failure retained.
import { createHash } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, writeSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  args: Bun.argv.slice(2),
  options: {
    baseline: { type: "string" },
    candidate: { type: "string" },
    output: { type: "string" },
    rounds: { type: "string", default: "9" },
    records: { type: "string", default: "2000000" },
    "serial-only": { type: "boolean", default: false },
    "dry-run": { type: "boolean", default: false },
  },
});
const rounds = Number(values.rounds);
const records = Number(values.records);
if (!values.baseline || !values.candidate || !values.output ||
    !Number.isSafeInteger(rounds) || rounds < 1 ||
    !Number.isSafeInteger(records) || records < 1) {
  throw new Error("Required: --baseline DIR --candidate DIR --output DIR; rounds/records must be positive integers");
}
const versions = { baseline: resolve(values.baseline), candidate: resolve(values.candidate) };
function plan(trial) {
  const rows = [];
  if (!values["serial-only"]) {
    const order = trial % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"];
    for (const version of order) {
      rows.push({ version, fixture: "bench_bus_perf", args: ["--mode", "both", "--shm-iters", "10000000", "--tcp-iters", "20000"], pin: true });
      rows.push({ version, fixture: "bench_market_perf", args: ["--orgs", "1024", "--products", "64", "--iters", "3000", "--warmup", "300"], pin: true });
    }
  }
  for (const producers of values["serial-only"] ? [0] : [0, 1, 3]) {
    const modes = [["baseline", "poll"], ["candidate", "poll"], ["candidate", "copy"]];
    const offset = ((trial % 3) + 3) % 3;
    for (const [version, mode] of [...modes.slice(offset), ...modes.slice(0, offset)]) {
      rows.push({ version, fixture: "bench_bus_mp_perf", args: [mode, String(producers), String(records)], pin: false });
    }
  }
  return rows;
}
if (values["dry-run"]) {
  console.log(JSON.stringify({ versions, rounds, warmup: 1, plans: [0, 1, 2].map(plan) }, null, 2));
  process.exit(0);
}
mkdirSync(values.output, { recursive: true });
// Refuse to overwrite previous measurements.
const output = openSync(join(values.output, "results.jsonl"), "wx");
let failures = 0;
try {
  for (let trial = -1; trial < rounds; trial++) {
    for (const row of plan(trial)) {
      const binary = join(versions[row.version], "tests", row.fixture);
      const command = [...(row.pin ? ["taskset", "-c", "6"] : []), binary, ...row.args];
      const begin = performance.now();
      const proc = Bun.spawn({ cmd: command, cwd: versions[row.version], stdout: "pipe", stderr: "pipe" });
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; proc.kill("SIGKILL"); }, 60000);
      let stdout, stderr, exitCode;
      try {
        [stdout, stderr, exitCode] = await Promise.all([
          new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited,
        ]);
      } finally {
        clearTimeout(timer);
      }
      const result = {
        trial, version: row.version, fixture: basename(binary), args: row.args,
        command, exit_code: timedOut ? 124 : exitCode, stdout, stderr,
        seconds: (performance.now() - begin) / 1000,
        binary_sha256: createHash("sha256").update(readFileSync(binary)).digest("hex"),
      };
      writeSync(output, JSON.stringify(result) + "\n");
      if (result.exit_code) {
        failures++;
        console.error("FAIL", trial, row.version, row.fixture, row.args, result.exit_code);
      }
    }
    console.log(trial < 0 ? "Warm-up complete" : `Completed round ${trial + 1} of ${rounds}`);
  }
} finally {
  closeSync(output);
}
// Failed legacy measurements are retained, but never turn into a passing gate.
process.exitCode = failures ? 1 : 0;
