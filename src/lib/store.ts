import { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { hostname } from "node:os";
import { dirname } from "node:path";
import type {
  ActionDeadLetter,
  ActionError,
  ActionInvocation,
  ActionResult,
  ApprovalDecision,
  ApprovalGate,
  JsonObject,
  JsonValue,
} from "@hasna/actions";
import { assertActionRunStatus, isTerminalActionStatus } from "@hasna/actions";
import type {
  ActionCompletionOptions,
  ActionFailureOptions,
  AutomationActionStep,
  AutomationRecord,
  AutomationReplayRequest,
  AutomationRun,
  AutomationsStatus,
  AutomationSpec,
  AutomationStatus,
  AutomationTrigger,
  EventEnvelopeLike,
  MaterializedEventRun,
  QueueClaimOptions,
  QueuedAction,
} from "../types.js";
import { AUTOMATION_SCHEMA_VERSION, AUTOMATION_STATUSES, AUTOMATION_TRIGGER_KINDS } from "../types.js";
import { automationsDataDir, automationsDbPath, ensureAutomationsDataDir } from "./paths.js";

const STORE_SCHEMA_VERSION = 1;

interface CountRow {
  count: number;
}

interface AutomationRow {
  id: string;
  spec_json: string;
  status: AutomationStatus;
  created_at: string;
  updated_at: string;
}

interface RunRow {
  id: string;
  automation_id: string;
  status: AutomationRun["status"];
  trigger_json: string;
  trigger_event_id: string | null;
  idempotency_key: string | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  metadata_json: string | null;
}

interface ActionRow {
  id: string;
  automation_run_id: string;
  step_id: string;
  action_id: string;
  idempotency_key: string | null;
  status: QueuedAction["status"];
  invocation_json: string;
  attempt: number;
  max_attempts: number;
  available_at: string;
  created_at: string;
  updated_at: string;
  claimed_by: string | null;
  claimed_at: string | null;
  lease_expires_at: string | null;
  approval_gate_json: string | null;
  result_json: string | null;
  error_json: string | null;
  dead_letter_json: string | null;
  metadata_json: string | null;
}

interface ReplayRow {
  id: string;
  source_run_id: string;
  requested_at: string;
  requested_by: string | null;
  mode: AutomationReplayRequest["mode"];
  reason: string | null;
  metadata_json: string | null;
}

interface DaemonLeaseRow {
  id: string;
  pid: number;
  hostname: string;
  heartbeat_at: string;
  expires_at: string;
  created_at: string;
  updated_at: string;
}

export interface AutomationsStoreOptions {
  dbPath?: string;
}

export interface EnqueueActionInput {
  id?: string;
  automationRunId: string;
  stepId: string;
  actionId: string;
  invocation: ActionInvocation<JsonValue>;
  idempotencyKey?: string;
  status?: QueuedAction["status"];
  attempt?: number;
  maxAttempts?: number;
  availableAt?: string | Date;
  approvalGate?: ApprovalGate;
  result?: ActionResult;
  error?: ActionError;
  deadLetter?: ActionDeadLetter;
  metadata?: JsonObject;
}

export class AutomationsStore {
  readonly db: Database;
  readonly path: string;

  constructor(options: AutomationsStoreOptions = {}) {
    ensureAutomationsDataDir();
    this.path = options.dbPath ?? automationsDbPath();
    mkdirSync(dirname(this.path), { recursive: true, mode: 0o700 });
    this.db = withBusyRetry(() => new Database(this.path, { create: true }));
    withBusyRetry(() => this.db.exec("PRAGMA busy_timeout = 10000;"));
    withBusyRetry(() => this.db.exec("PRAGMA foreign_keys = ON;"));
    withBusyRetry(() => this.db.exec("PRAGMA journal_mode = WAL;"));
    withBusyRetry(() => this.migrate());
  }

  close(): void {
    this.db.close();
  }

  createAutomation(spec: AutomationSpec): AutomationRecord {
    validateAutomationSpec(spec);
    const timestamp = nowIso();
    const status = spec.status ?? "active";
    this.db.query(`
      INSERT INTO automations (id, spec_json, status, created_at, updated_at)
      VALUES ($id, $specJson, $status, $createdAt, $updatedAt)
      ON CONFLICT(id) DO UPDATE SET
        spec_json = excluded.spec_json,
        status = excluded.status,
        updated_at = excluded.updated_at
    `).run({
      $id: spec.id,
      $specJson: JSON.stringify({ ...spec, status }),
      $status: status,
      $createdAt: timestamp,
      $updatedAt: timestamp,
    });
    return this.requireAutomation(spec.id);
  }

  listAutomations(): AutomationRecord[] {
    return this.db.query("SELECT * FROM automations ORDER BY created_at ASC").all().map((row) => automationFromRow(row as AutomationRow));
  }

  requireAutomation(id: string): AutomationRecord {
    const row = this.db.query("SELECT * FROM automations WHERE id = $id").get({ $id: id }) as AutomationRow | null;
    if (!row) throw new Error(`automation not found: ${id}`);
    return automationFromRow(row);
  }

  createRun(input: {
    id?: string;
    automationId: string;
    trigger: AutomationTrigger;
    triggerEventId?: string;
    idempotencyKey?: string;
    metadata?: JsonObject;
  }): AutomationRun {
    this.requireAutomation(input.automationId);
    if (input.idempotencyKey) {
      const existing = this.db.query(`
        SELECT * FROM automation_runs
        WHERE automation_id = $automationId AND idempotency_key = $idempotencyKey
        LIMIT 1
      `).get({ $automationId: input.automationId, $idempotencyKey: input.idempotencyKey }) as RunRow | null;
      if (existing) return runFromRow(existing);
    }
    const timestamp = nowIso();
    const id = input.id ?? randomUUID();
    this.db.query(`
      INSERT INTO automation_runs (
        id, automation_id, status, trigger_json, trigger_event_id, idempotency_key,
        created_at, updated_at, metadata_json
      )
      VALUES ($id, $automationId, 'materialized', $triggerJson, $triggerEventId, $idempotencyKey, $createdAt, $updatedAt, $metadataJson)
    `).run({
      $id: id,
      $automationId: input.automationId,
      $triggerJson: JSON.stringify(input.trigger),
      $triggerEventId: input.triggerEventId ?? null,
      $idempotencyKey: input.idempotencyKey ?? null,
      $createdAt: timestamp,
      $updatedAt: timestamp,
      $metadataJson: stringifyNullable(input.metadata),
    });
    return this.requireRun(id);
  }

  requireRun(id: string): AutomationRun {
    const row = this.db.query("SELECT * FROM automation_runs WHERE id = $id").get({ $id: id }) as RunRow | null;
    if (!row) throw new Error(`automation run not found: ${id}`);
    return runFromRow(row);
  }

  listRuns(): AutomationRun[] {
    return this.db.query("SELECT * FROM automation_runs ORDER BY created_at ASC").all().map((row) => runFromRow(row as RunRow));
  }

  enqueueAction(input: EnqueueActionInput): QueuedAction {
    this.requireRun(input.automationRunId);
    const timestamp = nowIso();
    const id = input.id ?? randomUUID();
    const idempotencyKey = input.idempotencyKey ?? input.invocation.idempotencyKey ?? `${input.automationRunId}:${input.stepId}`;
    const status = assertActionRunStatus(input.status ?? "queued");
    const existing = this.db.query(`
      SELECT * FROM automation_actions
      WHERE automation_run_id = $automationRunId
        AND (step_id = $stepId OR idempotency_key = $idempotencyKey)
      LIMIT 1
    `).get({ $automationRunId: input.automationRunId, $stepId: input.stepId, $idempotencyKey: idempotencyKey }) as ActionRow | null;
    if (existing) return actionFromRow(existing);
    this.db.query(`
      INSERT INTO automation_actions (
        id, automation_run_id, step_id, action_id, idempotency_key, status, invocation_json, attempt, max_attempts,
        available_at, created_at, updated_at, approval_gate_json, result_json, error_json, dead_letter_json, metadata_json
      )
      VALUES (
        $id, $automationRunId, $stepId, $actionId, $idempotencyKey, $status, $invocationJson, $attempt, $maxAttempts,
        $availableAt, $createdAt, $updatedAt, $approvalGateJson, $resultJson, $errorJson, $deadLetterJson, $metadataJson
      )
    `).run({
      $id: id,
      $automationRunId: input.automationRunId,
      $stepId: input.stepId,
      $actionId: input.actionId,
      $idempotencyKey: idempotencyKey,
      $status: status,
      $invocationJson: JSON.stringify(input.invocation),
      $attempt: input.attempt ?? 0,
      $maxAttempts: input.maxAttempts ?? 3,
      $availableAt: normalizeIso(input.availableAt),
      $createdAt: timestamp,
      $updatedAt: timestamp,
      $approvalGateJson: stringifyNullable(input.approvalGate),
      $resultJson: stringifyNullable(input.result),
      $errorJson: stringifyNullable(input.error),
      $deadLetterJson: stringifyNullable(input.deadLetter),
      $metadataJson: stringifyNullable(input.metadata),
    });
    return this.requireQueuedAction(id);
  }

  requireQueuedAction(id: string): QueuedAction {
    const row = this.db.query("SELECT * FROM automation_actions WHERE id = $id").get({ $id: id }) as ActionRow | null;
    if (!row) throw new Error(`queued action not found: ${id}`);
    return actionFromRow(row);
  }

  listQueuedActions(): QueuedAction[] {
    return this.db.query("SELECT * FROM automation_actions ORDER BY created_at ASC").all().map((row) => actionFromRow(row as ActionRow));
  }

  listDeadActions(): QueuedAction[] {
    return this.db.query("SELECT * FROM automation_actions WHERE status = 'dead' ORDER BY updated_at ASC").all().map((row) => actionFromRow(row as ActionRow));
  }

  claimNextAction(options: QueueClaimOptions): QueuedAction | undefined {
    const now = normalizeIso(options.now);
    const leaseExpiresAt = new Date(new Date(now).getTime() + (options.leaseMs ?? 30000)).toISOString();
    const claimedId = withImmediateTransaction(this.db, () => {
      let cursorAvailableAt: string | undefined;
      let cursorCreatedAt: string | undefined;
      let cursorId: string | undefined;
      while (true) {
        const rows = this.db.query(`
          SELECT * FROM automation_actions
          WHERE (
              status IN ('queued', 'retrying')
              OR (status = 'claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $now)
            )
            AND available_at <= $now
            AND (
              $cursorAvailableAt IS NULL
              OR available_at > $cursorAvailableAt
              OR (available_at = $cursorAvailableAt AND created_at > $cursorCreatedAt)
              OR (available_at = $cursorAvailableAt AND created_at = $cursorCreatedAt AND id > $cursorId)
            )
          ORDER BY available_at ASC, created_at ASC, id ASC
          LIMIT 100
        `).all({
          $now: now,
          $cursorAvailableAt: cursorAvailableAt ?? null,
          $cursorCreatedAt: cursorCreatedAt ?? null,
          $cursorId: cursorId ?? null,
        }) as ActionRow[];
        if (rows.length === 0) return undefined;
        for (const row of rows) {
          if (!approvalGateAllowsClaim(row.approval_gate_json)) {
            this.db.query(`
              UPDATE automation_actions
              SET status = 'waiting_approval', updated_at = $updatedAt
              WHERE id = $id AND status != 'waiting_approval'
            `).run({ $id: row.id, $updatedAt: now });
            continue;
          }
          if (!this.dependenciesSatisfied(row)) continue;
          const update = this.db.query(`
            UPDATE automation_actions
            SET status = 'claimed',
                claimed_by = $claimedBy,
                claimed_at = $claimedAt,
                lease_expires_at = $leaseExpiresAt,
                updated_at = $updatedAt
            WHERE id = $id
              AND (
                status IN ('queued', 'retrying')
                OR (status = 'claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $now)
              )
          `).run({
            $claimedBy: options.runnerId,
            $claimedAt: now,
            $leaseExpiresAt: leaseExpiresAt,
            $updatedAt: now,
            $id: row.id,
            $now: now,
          });
          if (update.changes > 0) return row.id;
        }
        const last = rows.at(-1)!;
        cursorAvailableAt = last.available_at;
        cursorCreatedAt = last.created_at;
        cursorId = last.id;
      }
    });
    return claimedId ? this.requireQueuedAction(claimedId) : undefined;
  }

  completeAction(options: ActionCompletionOptions): QueuedAction {
    const now = normalizeIso(options.now);
    return withImmediateTransaction(this.db, () => {
      const action = this.requireQueuedAction(options.actionId);
      if (isTerminalActionStatus(action.status)) {
        throw new Error(`cannot complete terminal queued action: ${options.actionId}`);
      }
      assertActiveLease(action, options.runnerId, now);
      const update = this.db.query(`
        UPDATE automation_actions
        SET status = 'succeeded',
            result_json = $resultJson,
            error_json = NULL,
            dead_letter_json = NULL,
            lease_expires_at = NULL,
            updated_at = $updatedAt
        WHERE id = $id
          AND status = 'claimed'
          AND claimed_by = $runnerId
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > $now
      `).run({
        $id: options.actionId,
        $runnerId: options.runnerId,
        $now: now,
        $resultJson: stringifyNullable(options.result),
        $updatedAt: now,
      });
      assertClaimedActionUpdated(update.changes, options.actionId, options.runnerId);
      return this.requireQueuedAction(options.actionId);
    });
  }

  failAction(options: ActionFailureOptions): QueuedAction {
    const now = normalizeIso(options.now);
    return withImmediateTransaction(this.db, () => {
      const action = this.requireQueuedAction(options.actionId);
      if (isTerminalActionStatus(action.status)) {
        throw new Error(`cannot fail terminal queued action: ${options.actionId}`);
      }
      assertActiveLease(action, options.runnerId, now);
      const nextAttempt = action.attempt + 1;
      const retryable = options.error.retryable !== false && nextAttempt < action.maxAttempts;
      if (retryable) {
        const availableAt = new Date(new Date(now).getTime() + (options.retryBackoffMs ?? defaultBackoffMs(nextAttempt))).toISOString();
        const update = this.db.query(`
          UPDATE automation_actions
          SET status = 'retrying',
              attempt = $attempt,
              available_at = $availableAt,
              error_json = $errorJson,
              lease_expires_at = NULL,
              updated_at = $updatedAt
          WHERE id = $id
            AND status = 'claimed'
            AND claimed_by = $runnerId
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at > $now
        `).run({
          $id: options.actionId,
          $runnerId: options.runnerId,
          $now: now,
          $attempt: nextAttempt,
          $availableAt: availableAt,
          $errorJson: JSON.stringify(options.error),
          $updatedAt: now,
        });
        assertClaimedActionUpdated(update.changes, options.actionId, options.runnerId);
      } else {
        const deadLetter: ActionDeadLetter = {
          reason: nextAttempt >= action.maxAttempts ? "max attempts exceeded" : "non-retryable action error",
          failedAt: now,
          lastError: options.error,
          attempts: nextAttempt,
          replayable: true,
        };
        const update = this.db.query(`
          UPDATE automation_actions
          SET status = 'dead',
              attempt = $attempt,
              error_json = $errorJson,
              dead_letter_json = $deadLetterJson,
              lease_expires_at = NULL,
              updated_at = $updatedAt
          WHERE id = $id
            AND status = 'claimed'
            AND claimed_by = $runnerId
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at > $now
        `).run({
          $id: options.actionId,
          $runnerId: options.runnerId,
          $now: now,
          $attempt: nextAttempt,
          $errorJson: JSON.stringify(options.error),
          $deadLetterJson: JSON.stringify(deadLetter),
          $updatedAt: now,
        });
        assertClaimedActionUpdated(update.changes, options.actionId, options.runnerId);
      }
      return this.requireQueuedAction(options.actionId);
    });
  }

  requeueDeadAction(id: string, options: { now?: string | Date; requestedBy?: string; reason?: string } = {}): QueuedAction {
    const action = this.requireQueuedAction(id);
    if (action.status !== "dead") throw new Error(`queued action is not dead: ${id}`);
    if (action.deadLetter?.replayable === false) throw new Error(`queued action is not replayable: ${id}`);
    this.createReplayRequest({
      sourceRunId: action.automationRunId,
      mode: "dead-actions",
      requestedBy: options.requestedBy,
      reason: options.reason ?? `requeue dead action ${id}`,
      requestedAt: options.now,
      metadata: { actionId: id },
    });
    const now = normalizeIso(options.now);
    this.db.query(`
      UPDATE automation_actions
      SET status = 'queued',
          attempt = 0,
          available_at = $availableAt,
          claimed_by = NULL,
          claimed_at = NULL,
          lease_expires_at = NULL,
          result_json = NULL,
          error_json = NULL,
          dead_letter_json = NULL,
          updated_at = $updatedAt
      WHERE id = $id
    `).run({ $id: id, $availableAt: now, $updatedAt: now });
    return this.requireQueuedAction(id);
  }

  approveAction(id: string, options: { now?: string | Date; decidedBy?: string; reason?: string } = {}): QueuedAction {
    const now = normalizeIso(options.now);
    return withImmediateTransaction(this.db, () => {
      const action = this.requireQueuedAction(id);
      assertApprovalTransitionAllowed(action, "approve");
      const decision: ApprovalDecision = {
        id: randomUUID(),
        status: "approved",
        requestedAt: action.approvalGate!.decision?.requestedAt ?? action.createdAt,
        decidedAt: now,
        reason: options.reason,
        metadata: options.decidedBy ? { decidedBy: options.decidedBy } : undefined,
      };
      const gate: ApprovalGate = {
        ...action.approvalGate!,
        blockedUntilApproved: false,
        decision,
      };
      const update = this.db.query(`
        UPDATE automation_actions
        SET status = 'queued',
            approval_gate_json = $approvalGateJson,
            updated_at = $updatedAt
        WHERE id = $id
          AND status IN ('queued', 'waiting_approval')
          AND claimed_by IS NULL
          AND claimed_at IS NULL
          AND lease_expires_at IS NULL
      `).run({
        $id: id,
        $approvalGateJson: JSON.stringify(gate),
        $updatedAt: now,
      });
      assertApprovalTransitionUpdated(update.changes, id);
      return this.requireQueuedAction(id);
    });
  }

  rejectAction(id: string, options: { now?: string | Date; decidedBy?: string; reason?: string } = {}): QueuedAction {
    const now = normalizeIso(options.now);
    return withImmediateTransaction(this.db, () => {
      const action = this.requireQueuedAction(id);
      assertApprovalTransitionAllowed(action, "reject");
      const decision: ApprovalDecision = {
        id: randomUUID(),
        status: "rejected",
        requestedAt: action.approvalGate!.decision?.requestedAt ?? action.createdAt,
        decidedAt: now,
        reason: options.reason,
        metadata: options.decidedBy ? { decidedBy: options.decidedBy } : undefined,
      };
      const gate: ApprovalGate = {
        ...action.approvalGate!,
        blockedUntilApproved: true,
        decision,
      };
      const deadLetter: ActionDeadLetter = {
        reason: options.reason ?? "approval rejected",
        failedAt: now,
        attempts: action.attempt,
        replayable: false,
        metadata: { approvalDecisionId: decision.id },
      };
      const update = this.db.query(`
        UPDATE automation_actions
        SET status = 'dead',
            approval_gate_json = $approvalGateJson,
            dead_letter_json = $deadLetterJson,
            updated_at = $updatedAt
        WHERE id = $id
          AND status IN ('queued', 'waiting_approval')
          AND claimed_by IS NULL
          AND claimed_at IS NULL
          AND lease_expires_at IS NULL
      `).run({
        $id: id,
        $approvalGateJson: JSON.stringify(gate),
        $deadLetterJson: JSON.stringify(deadLetter),
        $updatedAt: now,
      });
      assertApprovalTransitionUpdated(update.changes, id);
      return this.requireQueuedAction(id);
    });
  }

  materializeEvent(event: EventEnvelopeLike, options: { automationId?: string } = {}): MaterializedEventRun[] {
    const automations = this.listAutomations().filter((automation) => {
      if (options.automationId && automation.id !== options.automationId) return false;
      if (automation.status !== "active") return false;
      return automation.spec.triggers.some((trigger) => triggerMatchesEvent(trigger, event));
    });
    const materialized: MaterializedEventRun[] = [];
    for (const automation of automations) {
      const trigger = automation.spec.triggers.find((candidate) => triggerMatchesEvent(candidate, event));
      if (!trigger) continue;
      const run = this.createRun({
        automationId: automation.id,
        trigger,
        triggerEventId: event.id,
        idempotencyKey: `${automation.id}:${eventIdentityKey(event)}`,
        metadata: eventRunMetadata(event),
      });
      const actions = automation.spec.actions.map((step) => this.enqueueAction({
        automationRunId: run.id,
        stepId: step.id,
        actionId: step.actionId,
        invocation: {
          id: randomUUID(),
          actionId: step.actionId,
          manifestVersion: step.manifestVersion ?? "1.0.0",
          input: step.input ?? {},
          automationId: automation.id,
          runId: run.id,
          requestedAt: normalizeIso(event.time),
          idempotencyKey: `${automation.id}:${eventIdentityKey(event)}:${step.id}`,
          metadata: eventActionMetadata(event),
        },
        availableAt: normalizeIso(event.time),
        approvalGate: materializeApprovalGate(step, normalizeIso(event.time)),
      }));
      materialized.push({ automation, run, actions });
    }
    return materialized;
  }

  createReplayRequest(input: Omit<AutomationReplayRequest, "id" | "requestedAt"> & { id?: string; requestedAt?: string | Date }): AutomationReplayRequest {
    this.requireRun(input.sourceRunId);
    const id = input.id ?? randomUUID();
    const requestedAt = normalizeIso(input.requestedAt);
    this.db.query(`
      INSERT INTO automation_replay_requests (id, source_run_id, requested_at, requested_by, mode, reason, metadata_json)
      VALUES ($id, $sourceRunId, $requestedAt, $requestedBy, $mode, $reason, $metadataJson)
    `).run({
      $id: id,
      $sourceRunId: input.sourceRunId,
      $requestedAt: requestedAt,
      $requestedBy: input.requestedBy ?? null,
      $mode: input.mode,
      $reason: input.reason ?? null,
      $metadataJson: stringifyNullable(input.metadata),
    });
    return this.requireReplayRequest(id);
  }

  requireReplayRequest(id: string): AutomationReplayRequest {
    const row = this.db.query("SELECT * FROM automation_replay_requests WHERE id = $id").get({ $id: id }) as ReplayRow | null;
    if (!row) throw new Error(`replay request not found: ${id}`);
    return replayFromRow(row);
  }

  heartbeatDaemon(input: { leaseId?: string; ttlMs?: number; now?: Date } = {}): DaemonLeaseRow {
    const now = input.now ?? new Date();
    const timestamp = now.toISOString();
    const ttlMs = input.ttlMs ?? 30000;
    const id = input.leaseId ?? `daemon:${hostname()}:${process.pid}`;
    const expiresAt = new Date(now.getTime() + ttlMs).toISOString();
    this.db.query(`
      INSERT INTO daemon_leases (id, pid, hostname, heartbeat_at, expires_at, created_at, updated_at)
      VALUES ($id, $pid, $hostname, $heartbeatAt, $expiresAt, $createdAt, $updatedAt)
      ON CONFLICT(id) DO UPDATE SET
        pid = excluded.pid,
        hostname = excluded.hostname,
        heartbeat_at = excluded.heartbeat_at,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at
    `).run({
      $id: id,
      $pid: process.pid,
      $hostname: hostname(),
      $heartbeatAt: timestamp,
      $expiresAt: expiresAt,
      $createdAt: timestamp,
      $updatedAt: timestamp,
    });
    return this.latestDaemonLease()!;
  }

  latestDaemonLease(): DaemonLeaseRow | undefined {
    return this.db.query("SELECT * FROM daemon_leases ORDER BY updated_at DESC LIMIT 1").get() as DaemonLeaseRow | undefined;
  }

  status(now: Date = new Date()): AutomationsStatus {
    const lease = this.latestDaemonLease();
    return {
      service: "automations",
      schemaVersion: AUTOMATION_SCHEMA_VERSION,
      dataDir: automationsDataDir(),
      dbPath: this.path,
      counts: {
        automations: this.count("automations"),
        runs: this.count("automation_runs"),
        queuedActions: this.count("automation_actions"),
        deadActions: this.count("automation_actions", "status = 'dead'"),
        replayRequests: this.count("automation_replay_requests"),
      },
      daemon: {
        leaseId: lease?.id,
        pid: lease?.pid,
        hostname: lease?.hostname,
        heartbeatAt: lease?.heartbeat_at,
        expiresAt: lease?.expires_at,
        active: lease ? new Date(lease.expires_at).getTime() > now.getTime() : false,
      },
    };
  }

  private migrate(): void {
    const version = this.db.query("PRAGMA user_version").get() as { user_version: number };
    if (version.user_version > STORE_SCHEMA_VERSION) {
      throw new Error(`automations store schema ${version.user_version} is newer than supported schema ${STORE_SCHEMA_VERSION}`);
    }
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS automations (
        id TEXT PRIMARY KEY,
        spec_json TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS automation_runs (
        id TEXT PRIMARY KEY,
        automation_id TEXT NOT NULL,
        status TEXT NOT NULL,
        trigger_json TEXT NOT NULL,
        trigger_event_id TEXT,
        idempotency_key TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        error TEXT,
        metadata_json TEXT,
        FOREIGN KEY (automation_id) REFERENCES automations(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS automation_actions (
        id TEXT PRIMARY KEY,
        automation_run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        action_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        status TEXT NOT NULL,
        invocation_json TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL,
        available_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        claimed_by TEXT,
        claimed_at TEXT,
        lease_expires_at TEXT,
        approval_gate_json TEXT,
        result_json TEXT,
        error_json TEXT,
        dead_letter_json TEXT,
        metadata_json TEXT,
        FOREIGN KEY (automation_run_id) REFERENCES automation_runs(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS automation_replay_requests (
        id TEXT PRIMARY KEY,
        source_run_id TEXT NOT NULL,
        requested_at TEXT NOT NULL,
        requested_by TEXT,
        mode TEXT NOT NULL,
        reason TEXT,
        metadata_json TEXT,
        FOREIGN KEY (source_run_id) REFERENCES automation_runs(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS daemon_leases (
        id TEXT PRIMARY KEY,
        pid INTEGER NOT NULL,
        hostname TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);

    this.ensureColumn("automation_actions", "idempotency_key", "TEXT");
    this.ensureColumn("automation_actions", "result_json", "TEXT");
    this.ensureColumn("automation_actions", "error_json", "TEXT");
    this.db.exec(`
      UPDATE automation_actions
      SET idempotency_key = automation_run_id || ':' || step_id
      WHERE idempotency_key IS NULL;
      CREATE INDEX IF NOT EXISTS automation_runs_automation_idx ON automation_runs(automation_id);
      CREATE UNIQUE INDEX IF NOT EXISTS automation_runs_idempotency_idx
        ON automation_runs(automation_id, idempotency_key)
        WHERE idempotency_key IS NOT NULL;
      CREATE INDEX IF NOT EXISTS automation_actions_run_status_idx ON automation_actions(automation_run_id, status);
      CREATE INDEX IF NOT EXISTS automation_actions_available_idx ON automation_actions(status, available_at);
      CREATE UNIQUE INDEX IF NOT EXISTS automation_actions_idempotency_idx ON automation_actions(automation_run_id, idempotency_key);
      CREATE UNIQUE INDEX IF NOT EXISTS automation_actions_run_step_idx ON automation_actions(automation_run_id, step_id);
      CREATE INDEX IF NOT EXISTS automation_replay_source_idx ON automation_replay_requests(source_run_id);
      PRAGMA user_version = ${STORE_SCHEMA_VERSION};
    `);
  }

  private count(table: string, where?: string): number {
    const row = this.db.query(`SELECT COUNT(*) as count FROM ${table}${where ? ` WHERE ${where}` : ""}`).get() as CountRow;
    return row.count;
  }

  private dependenciesSatisfied(row: ActionRow): boolean {
    const run = this.requireRun(row.automation_run_id);
    const automation = this.requireAutomation(run.automationId);
    const step = automation.spec.actions.find((candidate) => candidate.id === row.step_id);
    const dependencies = step?.dependsOn ?? [];
    if (dependencies.length === 0) return true;
    const satisfied = this.db.query(`
      SELECT step_id FROM automation_actions
      WHERE automation_run_id = $runId AND status = 'succeeded'
    `).all({ $runId: row.automation_run_id }) as Array<{ step_id: string }>;
    const succeeded = new Set(satisfied.map((candidate) => candidate.step_id));
    return dependencies.every((dependency) => succeeded.has(dependency));
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const rows = this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
    if (rows.some((row) => row.name === column)) return;
    this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition};`);
  }
}

export function validateAutomationSpec(spec: AutomationSpec): void {
  if (spec.schemaVersion !== AUTOMATION_SCHEMA_VERSION) {
    throw new Error(`unsupported automation schemaVersion: ${spec.schemaVersion}`);
  }
  if (!spec.id || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/.test(spec.id)) {
    throw new Error("automation id must start with an alphanumeric character and contain only letters, numbers, dots, underscores, colons, or dashes");
  }
  if (!spec.name) throw new Error("automation name is required");
  if (!spec.version) throw new Error("automation version is required");
  if (spec.status && !(AUTOMATION_STATUSES as readonly string[]).includes(spec.status)) {
    throw new Error(`unsupported automation status: ${spec.status}`);
  }
  if (!Array.isArray(spec.triggers) || spec.triggers.length === 0) throw new Error("automation requires at least one trigger");
  for (const trigger of spec.triggers) {
    if (!(AUTOMATION_TRIGGER_KINDS as readonly string[]).includes(trigger.kind)) {
      throw new Error(`unsupported automation trigger kind: ${trigger.kind}`);
    }
  }
  if (!Array.isArray(spec.actions) || spec.actions.length === 0) throw new Error("automation requires at least one action step");
  const stepIds = new Set<string>();
  for (const action of spec.actions) {
    if (!action.id || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/.test(action.id)) {
      throw new Error("automation action step id must start with an alphanumeric character and contain only letters, numbers, dots, underscores, colons, or dashes");
    }
    if (stepIds.has(action.id)) throw new Error(`duplicate automation action step id: ${action.id}`);
    stepIds.add(action.id);
    if (!action.actionId || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]*$/.test(action.actionId)) {
      throw new Error(`automation action step ${action.id} requires a valid actionId`);
    }
    if (action.dependsOn !== undefined && !Array.isArray(action.dependsOn)) {
      throw new Error(`automation action step ${action.id} dependsOn must be an array`);
    }
    if (action.approval && action.approvalGate) {
      throw new Error(`automation action step ${action.id} cannot define both approval and approvalGate`);
    }
    if (action.approval && typeof action.approval.requiresApproval !== "boolean") {
      throw new Error(`automation action step ${action.id} approval.requiresApproval must be a boolean`);
    }
    if (action.approvalGate) {
      if (action.approvalGate.decision !== undefined) {
        throw new Error(`automation action step ${action.id} approval gate templates cannot include decisions`);
      }
      if (typeof action.approvalGate.blockedUntilApproved !== "boolean") {
        throw new Error(`automation action step ${action.id} approvalGate.blockedUntilApproved must be a boolean`);
      }
    }
  }
  for (const action of spec.actions) {
    for (const dependency of action.dependsOn ?? []) {
      if (dependency === action.id) throw new Error(`automation action step ${action.id} cannot depend on itself`);
      if (!stepIds.has(dependency)) throw new Error(`automation action step ${action.id} depends on unknown step: ${dependency}`);
    }
  }
  if (spec.concurrency?.limit !== undefined && (!Number.isInteger(spec.concurrency.limit) || spec.concurrency.limit < 1)) {
    throw new Error("automation concurrency limit must be a positive integer");
  }
}

export function exampleAutomationSpec(): AutomationSpec {
  return {
    schemaVersion: AUTOMATION_SCHEMA_VERSION,
    id: "tickets.escalate-critical",
    name: "Escalate critical tickets",
    version: "1.0.0",
    description: "Materialize critical ticket events into deterministic action runs.",
    status: "active",
    triggers: [
      {
        kind: "event",
        source: "open-events",
        type: "ticket.created",
        filter: { priority: "critical" },
      },
    ],
    actions: [
      {
        id: "create-escalation-task",
        actionId: "todos.create",
        manifestVersion: "1.0.0",
        input: {
          title: "Escalate critical ticket",
        },
      },
    ],
    audit: {
      eventSource: "hasna.automations",
    },
  };
}

function automationFromRow(row: AutomationRow): AutomationRecord {
  return {
    id: row.id,
    spec: JSON.parse(row.spec_json) as AutomationSpec,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function runFromRow(row: RunRow): AutomationRun {
  return pruneUndefined({
    id: row.id,
    automationId: row.automation_id,
    status: row.status,
    trigger: JSON.parse(row.trigger_json) as AutomationTrigger,
    triggerEventId: row.trigger_event_id ?? undefined,
    idempotencyKey: row.idempotency_key ?? undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at ?? undefined,
    completedAt: row.completed_at ?? undefined,
    error: row.error ?? undefined,
    metadata: parseNullable<JsonObject>(row.metadata_json),
  });
}

function actionFromRow(row: ActionRow): QueuedAction {
  const idempotencyKey = row.idempotency_key ?? `${row.automation_run_id}:${row.step_id}`;
  return pruneUndefined({
    id: row.id,
    automationRunId: row.automation_run_id,
    stepId: row.step_id,
    actionId: row.action_id,
    idempotencyKey,
    status: row.status,
    invocation: JSON.parse(row.invocation_json) as ActionInvocation,
    attempt: row.attempt,
    maxAttempts: row.max_attempts,
    availableAt: row.available_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    claimedBy: row.claimed_by ?? undefined,
    claimedAt: row.claimed_at ?? undefined,
    leaseExpiresAt: row.lease_expires_at ?? undefined,
    approvalGate: parseNullable<ApprovalGate>(row.approval_gate_json),
    result: parseNullable<ActionResult>(row.result_json),
    error: parseNullable<ActionError>(row.error_json),
    deadLetter: parseNullable<ActionDeadLetter>(row.dead_letter_json),
    metadata: parseNullable<JsonObject>(row.metadata_json),
  });
}

function replayFromRow(row: ReplayRow): AutomationReplayRequest {
  return pruneUndefined({
    id: row.id,
    sourceRunId: row.source_run_id,
    requestedAt: row.requested_at,
    requestedBy: row.requested_by ?? undefined,
    mode: row.mode,
    reason: row.reason ?? undefined,
    metadata: parseNullable<JsonObject>(row.metadata_json),
  });
}

function assertActiveLease(action: QueuedAction, runnerId: string, now: string): void {
  if (action.status !== "claimed") {
    throw new Error(`queued action is not claimed: ${action.id}`);
  }
  if (action.claimedBy !== runnerId) {
    throw new Error(`queued action ${action.id} is claimed by ${action.claimedBy ?? "unknown"}, not ${runnerId}`);
  }
  if (!action.leaseExpiresAt || new Date(action.leaseExpiresAt).getTime() <= new Date(now).getTime()) {
    throw new Error(`queued action lease expired: ${action.id}`);
  }
}

function assertClaimedActionUpdated(changes: number, actionId: string, runnerId: string): void {
  if (changes === 0) {
    throw new Error(`queued action lease is no longer active for runner ${runnerId}: ${actionId}`);
  }
}

function assertApprovalTransitionAllowed(action: QueuedAction, operation: "approve" | "reject"): void {
  if (!action.approvalGate) throw new Error(`queued action has no approval gate: ${action.id}`);
  if (isTerminalActionStatus(action.status)) {
    throw new Error(`cannot ${operation} terminal queued action: ${action.id}`);
  }
  if (action.status === "claimed" || action.claimedBy || action.claimedAt || action.leaseExpiresAt) {
    throw new Error(`cannot ${operation} claimed queued action: ${action.id}`);
  }
  if (action.status !== "queued" && action.status !== "waiting_approval") {
    throw new Error(`queued action is not awaiting approval: ${action.id}`);
  }
  if (action.approvalGate.decision?.status !== "pending") {
    throw new Error(`queued action approval decision is not pending: ${action.id}`);
  }
}

function assertApprovalTransitionUpdated(changes: number, actionId: string): void {
  if (changes === 0) {
    throw new Error(`queued action approval state changed before transition: ${actionId}`);
  }
}

function approvalGateAllowsClaim(value: string | null): boolean {
  if (!value) return true;
  const gate = JSON.parse(value) as ApprovalGate;
  if (!gate.requirement.requiresApproval) return true;
  return gate.blockedUntilApproved === false && gate.decision?.status === "approved";
}

function materializeApprovalGate(step: AutomationActionStep, requestedAt: string): ApprovalGate | undefined {
  const requirement = step.approval ?? step.approvalGate?.requirement;
  if (!requirement?.requiresApproval) return undefined;
  return {
    requirement,
    blockedUntilApproved: true,
    decision: {
      id: randomUUID(),
      status: "pending",
      requestedAt,
    },
  };
}

function eventIdentityKey(event: EventEnvelopeLike): string {
  return event.dedupeKey ?? event.id;
}

function eventRunMetadata(event: EventEnvelopeLike): JsonObject {
  const metadata: JsonObject = {
    eventSource: event.source,
    eventType: event.type,
  };
  if (event.dedupeKey) metadata.eventDedupeKey = event.dedupeKey;
  return metadata;
}

function eventActionMetadata(event: EventEnvelopeLike): JsonObject {
  const metadata: JsonObject = {
    eventId: event.id,
  };
  if (event.dedupeKey) metadata.eventDedupeKey = event.dedupeKey;
  return metadata;
}

function defaultBackoffMs(attempt: number): number {
  return Math.min(60000, 1000 * 2 ** Math.max(0, attempt - 1));
}

function triggerMatchesEvent(trigger: AutomationTrigger, event: EventEnvelopeLike): boolean {
  if (trigger.kind !== "event") return false;
  if (trigger.source && trigger.source !== event.source) return false;
  if (trigger.type && trigger.type !== event.type) return false;
  if (trigger.subject && trigger.subject !== event.subject) return false;
  return objectFilterMatches(trigger.filter, event.data ?? {});
}

function objectFilterMatches(filter: JsonObject | undefined, data: JsonObject): boolean {
  if (!filter) return true;
  return Object.entries(filter).every(([key, expected]) => {
    const observed = data[key];
    if (isPlainObject(expected) && "not" in expected) {
      return observed !== expected.not;
    }
    return observed === expected;
  });
}

function isPlainObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nowIso(): string {
  return new Date().toISOString();
}

function normalizeIso(value?: string | Date): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return new Date(value).toISOString();
  return nowIso();
}

function stringifyNullable(value: unknown): string | null {
  return value === undefined ? null : JSON.stringify(value);
}

function parseNullable<T>(value: string | null): T | undefined {
  return value === null ? undefined : JSON.parse(value) as T;
}

function pruneUndefined<T extends object>(input: T): T {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined)) as T;
}

function withImmediateTransaction<T>(db: Database, fn: () => T): T {
  return withBusyRetry(() => {
    db.exec("BEGIN IMMEDIATE;");
    try {
      const result = fn();
      db.exec("COMMIT;");
      return result;
    } catch (error) {
      db.exec("ROLLBACK;");
      throw error;
    }
  });
}

function withBusyRetry<T>(fn: () => T): T {
  let lastError: unknown;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return fn();
    } catch (error) {
      if (!isBusyError(error)) throw error;
      lastError = error;
      sleepSync(Math.min(500, 25 * (attempt + 1)));
    }
  }
  throw lastError;
}

function isBusyError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes("database is locked") || message.includes("SQLITE_BUSY") || message.includes("SQLITE_LOCKED");
}

function sleepSync(ms: number): void {
  const buffer = new SharedArrayBuffer(4);
  const array = new Int32Array(buffer);
  Atomics.wait(array, 0, 0, ms);
}
