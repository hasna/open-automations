import type {
  ActionDeadLetter,
  ActionError,
  ActionInvocation,
  ActionResult,
  ActionRunStatus,
  ApprovalGate,
  ApprovalRequirement,
  JsonObject,
  JsonValue,
} from "@hasna/actions";

export const AUTOMATION_SCHEMA_VERSION = "1.0" as const;

export const AUTOMATION_STATUSES = [
  "active",
  "paused",
  "archived",
] as const;

export type AutomationStatus = (typeof AUTOMATION_STATUSES)[number];

export const AUTOMATION_RUN_STATUSES = [
  "pending",
  "materialized",
  "running",
  "succeeded",
  "failed",
  "cancelled",
  "dead",
] as const;

export type AutomationRunStatus = (typeof AUTOMATION_RUN_STATUSES)[number];

export const AUTOMATION_TRIGGER_KINDS = [
  "manual",
  "event",
  "webhook",
  "schedule",
  "api",
] as const;

export type AutomationTriggerKind = (typeof AUTOMATION_TRIGGER_KINDS)[number];

export interface AutomationTrigger {
  kind: AutomationTriggerKind;
  source?: string;
  type?: string;
  subject?: string;
  filter?: JsonObject;
  metadata?: JsonObject;
}

export interface EventEnvelopeLike<TData extends JsonObject = JsonObject> {
  id: string;
  source: string;
  type: string;
  time?: string;
  subject?: string;
  data?: TData;
  dedupeKey?: string;
  metadata?: JsonObject;
}

export type AutomationApprovalGateTemplate = Omit<ApprovalGate, "decision"> & {
  decision?: never;
};

export interface AutomationActionStep {
  id: string;
  actionId: string;
  manifestVersion?: string;
  input?: JsonValue;
  dependsOn?: string[];
  when?: JsonObject;
  approval?: ApprovalRequirement;
  approvalGate?: AutomationApprovalGateTemplate;
  metadata?: JsonObject;
}

export interface AutomationSpec {
  schemaVersion: typeof AUTOMATION_SCHEMA_VERSION;
  id: string;
  name: string;
  version: string;
  description?: string;
  status?: AutomationStatus;
  triggers: AutomationTrigger[];
  actions: AutomationActionStep[];
  concurrency?: {
    key?: string;
    limit?: number;
  };
  audit?: {
    eventSource?: string;
    evidenceRefs?: string[];
  };
  metadata?: JsonObject;
}

export interface AutomationRecord {
  id: string;
  spec: AutomationSpec;
  status: AutomationStatus;
  createdAt: string;
  updatedAt: string;
}

export interface AutomationRun {
  id: string;
  automationId: string;
  status: AutomationRunStatus;
  trigger: AutomationTrigger;
  triggerEventId?: string;
  idempotencyKey?: string;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  completedAt?: string;
  error?: string;
  metadata?: JsonObject;
}

export interface QueuedAction {
  id: string;
  automationRunId: string;
  stepId: string;
  actionId: string;
  idempotencyKey: string;
  status: ActionRunStatus;
  invocation: ActionInvocation<JsonValue>;
  attempt: number;
  maxAttempts: number;
  availableAt: string;
  createdAt: string;
  updatedAt: string;
  claimedBy?: string;
  claimedAt?: string;
  leaseExpiresAt?: string;
  approvalGate?: ApprovalGate;
  result?: ActionResult;
  error?: ActionError;
  deadLetter?: ActionDeadLetter;
  metadata?: JsonObject;
}

export interface AutomationReplayRequest {
  id: string;
  sourceRunId: string;
  requestedAt: string;
  requestedBy?: string;
  mode: "failed-actions" | "dead-actions" | "entire-run";
  reason?: string;
  metadata?: JsonObject;
}

export interface AutomationsStatus {
  service: "automations";
  schemaVersion: typeof AUTOMATION_SCHEMA_VERSION;
  dataDir: string;
  dbPath: string;
  counts: {
    automations: number;
    runs: number;
    queuedActions: number;
    deadActions: number;
    replayRequests: number;
  };
  daemon: {
    leaseId?: string;
    pid?: number;
    hostname?: string;
    heartbeatAt?: string;
    expiresAt?: string;
    active: boolean;
  };
}

export interface QueueClaimOptions {
  runnerId: string;
  leaseMs?: number;
  now?: string | Date;
}

export interface ActionFailureOptions {
  actionId: string;
  runnerId: string;
  error: ActionError;
  now?: string | Date;
  retryBackoffMs?: number;
}

export interface ActionCompletionOptions {
  actionId: string;
  runnerId: string;
  result?: ActionResult;
  now?: string | Date;
}

export interface MaterializedEventRun {
  automation: AutomationRecord;
  run: AutomationRun;
  actions: QueuedAction[];
}

export interface AutomationRuntimeBinding {
  kind: "open-loops" | "local" | "external";
  name: string;
  description?: string;
  handoff: "claim-queue" | "webhook" | "sdk";
  metadata?: JsonObject;
}
