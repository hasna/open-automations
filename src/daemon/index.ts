#!/usr/bin/env bun
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { AutomationsStore } from "../index.js";
import { daemonPidFilePath, ensureAutomationsDataDir } from "../lib/paths.js";

interface ParsedArgs {
  json: boolean;
  dir?: string;
  rest: string[];
}

interface DaemonRunOptions {
  once: boolean;
  intervalMs: number;
  ttlMs: number;
}

export async function runAutomationsDaemonCli(argv = Bun.argv.slice(2)): Promise<number> {
  const parsed = parseGlobalArgs(argv);
  if (parsed.dir) process.env.HASNA_AUTOMATIONS_DIR = parsed.dir;
  const command = parsed.rest[0];

  try {
    if (!command || command === "--help" || command === "-h" || command === "help") {
      printHelp();
      return 0;
    }
    if (command === "--version" || command === "-v" || command === "version") {
      output(parsed, { version: packageVersion() }, () => console.log(packageVersion()));
      return 0;
    }
    if (command === "status") {
      const store = new AutomationsStore();
      try {
        output(parsed, store.status(), () => console.log(JSON.stringify(store.status(), null, 2)));
      } finally {
        store.close();
      }
      return 0;
    }
    if (command === "run") {
      return runDaemon(parsed);
    }
    throw new Error(`Unknown command: ${command}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (parsed.json) {
      console.error(JSON.stringify({ ok: false, error: message }, null, 2));
    } else {
      console.error(`automations-daemon: ${message}`);
    }
    return 1;
  }
}

export async function runDaemon(parsed: ParsedArgs): Promise<number> {
  ensureAutomationsDataDir();
  writeFileSync(daemonPidFilePath(), `${process.pid}\n`, { mode: 0o600 });
  const options = parseRunOptions(parsed.rest.slice(1));
  const store = new AutomationsStore();
  let stopping = false;
  const stop = () => {
    stopping = true;
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    let first = true;
    while (!stopping) {
      const lease = store.heartbeatDaemon({ ttlMs: options.ttlMs });
      if (first || options.once) {
        output(parsed, { ok: true, leaseId: lease.id, pid: lease.pid, heartbeatAt: lease.heartbeat_at, once: options.once }, () => {
          console.log(`automations-daemon heartbeat ${lease.id}`);
        });
      }
      if (options.once) return 0;
      first = false;
      await Bun.sleep(options.intervalMs);
    }
    return 0;
  } finally {
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    store.close();
  }
}

function parseRunOptions(args: string[]): DaemonRunOptions {
  const options: DaemonRunOptions = {
    once: false,
    intervalMs: 5000,
    ttlMs: 15000,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--once") {
      options.once = true;
    } else if (arg.startsWith("--interval-ms=")) {
      options.intervalMs = Number(arg.slice("--interval-ms=".length));
    } else if (arg === "--interval-ms") {
      options.intervalMs = Number(args[++index]);
    } else if (arg.startsWith("--ttl-ms=")) {
      options.ttlMs = Number(arg.slice("--ttl-ms=".length));
    } else if (arg === "--ttl-ms") {
      options.ttlMs = Number(args[++index]);
    } else {
      throw new Error(`Unknown run option: ${arg}`);
    }
  }
  if (!Number.isFinite(options.intervalMs) || options.intervalMs < 100) {
    throw new Error("--interval-ms must be at least 100");
  }
  if (!Number.isFinite(options.ttlMs) || options.ttlMs < options.intervalMs) {
    throw new Error("--ttl-ms must be greater than or equal to --interval-ms");
  }
  return options;
}

function parseGlobalArgs(argv: string[]): ParsedArgs {
  const rest: string[] = [];
  let json = false;
  let dir: string | undefined;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--") {
      rest.push(...argv.slice(index + 1));
      break;
    }
    if (arg === "--json" || arg === "-j") {
      json = true;
      continue;
    }
    if (arg.startsWith("--dir=")) {
      dir = arg.slice("--dir=".length);
      continue;
    }
    if (arg === "--dir") {
      dir = argv[++index];
      continue;
    }
    rest.push(...argv.slice(index));
    break;
  }
  return { json, dir, rest };
}

function output(parsed: ParsedArgs, value: unknown, human: () => void): void {
  if (parsed.json) {
    console.log(JSON.stringify(value, null, 2));
    return;
  }
  human();
}

function printHelp(): void {
  console.log(`automations-daemon ${packageVersion()}

Usage:
  automations-daemon [--dir <path>] [--json] status
  automations-daemon [--dir <path>] [--json] run [--once] [--interval-ms <ms>] [--ttl-ms <ms>]`);
}

function packageVersion(): string {
  try {
    const packagePath = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "package.json");
    return JSON.parse(readFileSync(packagePath, "utf-8")).version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
}

if (import.meta.main) {
  process.exit(await runAutomationsDaemonCli());
}
