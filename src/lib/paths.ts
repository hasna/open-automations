import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export function automationsDataDir(): string {
  return process.env.HASNA_AUTOMATIONS_DIR || process.env.AUTOMATIONS_DATA_DIR || join(homedir(), ".hasna", "automations");
}

export function ensureAutomationsDataDir(): string {
  const dir = automationsDataDir();
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  return dir;
}

export function automationsDbPath(): string {
  return join(automationsDataDir(), "automations.db");
}

export function daemonPidFilePath(): string {
  return join(automationsDataDir(), "daemon.pid");
}

export function daemonLogPath(): string {
  return join(automationsDataDir(), "daemon.log");
}
