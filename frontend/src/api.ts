import type { Approval, ApprovalIntent, ApprovalPage, CorrelationTrace, OperationalItem, OperationalPage, OperationKind, Task, TaskPage, TaskStatus } from "./types";

export class AgentBusUnavailable extends Error {}

type Decoder<T> = (value: unknown) => value is T;

const taskStatuses = new Set<TaskStatus>([
  "ready", "running", "waiting_approval", "succeeded", "failed", "cancelled",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNullableString(value: unknown): value is string | null {
  return typeof value === "string" || value === null;
}

function isTask(value: unknown): value is Task {
  if (!isRecord(value)) return false;
  return typeof value.id === "string"
    && typeof value.type === "string"
    && typeof value.title === "string"
    && isNullableString(value.description)
    && isRecord(value.input)
    && (value.output === null || isRecord(value.output))
    && typeof value.status === "string"
    && taskStatuses.has(value.status as TaskStatus)
    && typeof value.requested_by === "string"
    && isNullableString(value.assigned_to)
    && isNullableString(value.failure_code)
    && isNullableString(value.failure_message)
    && isNullableString(value.retry_of)
    && isNullableString(value.waiting_on_approval_id)
    && typeof value.correlation_id === "string"
    && Number.isInteger(value.version) && (value.version as number) >= 1
    && typeof value.created_at === "string"
    && typeof value.updated_at === "string"
    && isNullableString(value.started_at)
    && isNullableString(value.finished_at);
}

function isEvent(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return typeof value.id === "string"
    && typeof value.task_id === "string"
    && Number.isInteger(value.sequence) && (value.sequence as number) >= 1
    && typeof value.type === "string"
    && isRecord(value.data)
    && typeof value.actor_id === "string"
    && isNullableString(value.authenticated_principal_id)
    && typeof value.correlation_id === "string"
    && isNullableString(value.causation_event_id)
    && typeof value.command_id === "string"
    && Number.isInteger(value.event_index) && (value.event_index as number) >= 0
    && typeof value.occurred_at === "string";
}

function isTaskPage(value: unknown): value is TaskPage {
  return isRecord(value)
    && Array.isArray(value.items)
    && value.items.every(isTask)
    && isNullableString(value.next_cursor);
}

function isCorrelationTrace(value: unknown): value is CorrelationTrace {
  return isRecord(value)
    && typeof value.correlation_id === "string"
    && Array.isArray(value.tasks)
    && value.tasks.every(isTask)
    && Array.isArray(value.events)
    && value.events.every(isEvent);
}

function isApproval(value: unknown): value is Approval {
  return isRecord(value) && typeof value.id === "string" && typeof value.task_id === "string"
    && typeof value.gate === "string" && ["pending", "approved", "rejected"].includes(String(value.status))
    && typeof value.request_reason === "string" && isRecord(value.context)
    && typeof value.requested_by === "string" && isNullableString(value.assigned_to)
    && isNullableString(value.decided_by) && isNullableString(value.decision_reason)
    && typeof value.correlation_id === "string" && Number.isInteger(value.version)
    && typeof value.created_at === "string" && typeof value.updated_at === "string"
    && isNullableString(value.decided_at);
}

function isApprovalPage(value: unknown): value is ApprovalPage {
  return isRecord(value) && Array.isArray(value.items) && value.items.every(isApproval)
    && isNullableString(value.next_cursor);
}

function isApprovalIntent(value: unknown): value is ApprovalIntent {
  return isRecord(value) && typeof value.intent_id === "string" && isRecord(value.approval)
    && typeof value.approval.id === "string" && typeof value.approval.task_id === "string"
    && typeof value.approval.gate === "string" && typeof value.approval.request_reason === "string"
    && isNullableString(value.approval.assigned_to) && Number.isInteger(value.approval.version)
    && ["approve", "reject"].includes(String(value.decision))
    && ["pending", "confirmed", "sending", "completed", "stopped", "expired"].includes(String(value.state))
    && isNullableString(value.decision_reason) && typeof value.created_at === "string"
    && isNullableString(value.confirmed_at) && isNullableString(value.completed_at)
    && (value.result === null || isApproval(value.result)) && isNullableString(value.public_error)
    && (typeof value.replayed === "boolean" || value.replayed === null);
}

function isOperationalItem(value: unknown): value is OperationalItem {
  return isRecord(value) && typeof value.operation_id === "string" && typeof value.source_id === "string"
    && ["approval_intent", "release", "runner", "outbox"].includes(String(value.kind))
    && typeof value.persisted_state === "string" && typeof value.attention_class === "string"
    && typeof value.recovery_mode === "string" && isNullableString(value.task_id)
    && isNullableString(value.approval_id) && isNullableString(value.correlation_id)
    && typeof value.logical_owner === "string" && isNullableString(value.created_at)
    && isNullableString(value.state_observed_at) && ["exact", "lower_bound", "unknown"].includes(String(value.timestamp_quality))
    && (value.last_http_status === null || Number.isInteger(value.last_http_status))
    && isNullableString(value.public_error_code) && Array.isArray(value.evidence)
    && value.evidence.every((entry) => typeof entry === "string") && typeof value.summary === "string";
}

function isOperationalPage(value: unknown): value is OperationalPage {
  return isRecord(value) && Array.isArray(value.items) && value.items.every(isOperationalItem)
    && isNullableString(value.next_cursor) && value.snapshot_scope === "per_request_not_global"
    && Array.isArray(value.sources) && value.sources.every((source) => isRecord(source)
      && typeof source.source_id === "string" && ["approval_intent", "release", "runner", "outbox"].includes(String(source.kind))
      && ["available", "unavailable", "incompatible"].includes(String(source.status))
      && typeof source.observed_at === "string" && isNullableString(source.public_error_code));
}

async function readJson<T>(
  path: string,
  decoder: Decoder<T>,
  signal?: AbortSignal,
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, { signal });
  } catch {
    throw new AgentBusUnavailable("O AgentBus local não respondeu.");
  }
  if (!response.ok) {
    let detail = `AgentBus respondeu HTTP ${response.status}.`;
    try {
      const body = await response.json();
      detail = body?.detail?.message ?? body?.detail ?? detail;
    } catch {
      // The status remains authoritative when the body is not JSON.
    }
    throw new Error(String(detail));
  }
  try {
    const body: unknown = await response.json();
    if (!decoder(body)) throw new Error("invalid contract");
    return body;
  } catch {
    throw new Error("O AgentBus devolveu uma resposta inválida.");
  }
}

export interface TaskFilters {
  status?: TaskStatus;
  assignedTo?: string;
  requestedBy?: string;
  type?: string;
  cursor?: string;
  limit?: number;
}

export function listTasks(filters: TaskFilters, signal?: AbortSignal): Promise<TaskPage> {
  const params = new URLSearchParams();
  params.set("limit", String(filters.limit ?? 20));
  if (filters.status) params.set("status", filters.status);
  if (filters.assignedTo) params.set("assigned_to", filters.assignedTo);
  if (filters.requestedBy) params.set("requested_by", filters.requestedBy);
  if (filters.type) params.set("type", filters.type);
  if (filters.cursor) params.set("cursor", filters.cursor);
  return readJson(`/tasks?${params}`, isTaskPage, signal);
}

export function getTask(id: string, signal?: AbortSignal): Promise<Task> {
  return readJson(`/tasks/${encodeURIComponent(id)}`, isTask, signal);
}

export function getTrace(id: string, signal?: AbortSignal): Promise<CorrelationTrace> {
  return readJson(
    `/correlations/${encodeURIComponent(id)}/trace`,
    isCorrelationTrace,
    signal,
  );
}

export interface ApprovalFilters { assignedTo?: string; status?: string; cursor?: string; limit?: number; }

export function listApprovals(filters: ApprovalFilters, signal?: AbortSignal): Promise<ApprovalPage> {
  const params = new URLSearchParams({ limit: String(filters.limit ?? 20) });
  if (filters.assignedTo) params.set("assigned_to", filters.assignedTo);
  if (filters.status) params.set("status", filters.status);
  if (filters.cursor) params.set("cursor", filters.cursor);
  return readJson(`/approvals?${params}`, isApprovalPage, signal);
}

export interface OperationFilters { kind?: OperationKind; attentionClass?: string; cursor?: string; limit?: number; }
export function listOperations(filters: OperationFilters, signal?: AbortSignal): Promise<OperationalPage> {
  const params = new URLSearchParams({ limit: String(filters.limit ?? 20) });
  if (filters.kind) params.set("kind", filters.kind);
  if (filters.attentionClass) params.set("attention_class", filters.attentionClass);
  if (filters.cursor) params.set("cursor", filters.cursor);
  return readJson(`/operations?${params}`, isOperationalPage, signal);
}
export function getOperation(id: string, signal?: AbortSignal): Promise<OperationalItem> {
  return readJson(`/operations/${encodeURIComponent(id)}`, isOperationalItem, signal);
}

let csrfToken: string | null = null;

async function intentSession(refresh = false): Promise<string> {
  if (csrfToken && !refresh) return csrfToken;
  const response = await fetch("/intent-api/session");
  if (!response.ok) throw new Error("Não foi possível iniciar a sessão local de intenção.");
  const body: unknown = await response.json();
  if (!isRecord(body) || typeof body.csrf_token !== "string") throw new Error("Sessão de intenção inválida.");
  csrfToken = body.csrf_token;
  return csrfToken;
}

export async function createApprovalIntent(
  approval: Approval, decision: "approve" | "reject", reason: string | null,
): Promise<ApprovalIntent> {
  const intentKey = crypto.randomUUID();
  const requestBody = JSON.stringify({ approval_id: approval.id, decision, decision_reason: reason, if_match: `"v${approval.version}"` });
  const send = async (csrf: string) => fetch("/intent-api/approval-intents", {
    method: "POST", headers: { "Content-Type": "application/json", "X-AgentBus-CSRF": csrf, "Intent-Key": intentKey }, body: requestBody,
  });
  let response = await send(await intentSession());
  if (response.status === 403) response = await send(await intentSession(true));
  if (!response.ok) throw new Error(`Não foi possível registrar a intenção (HTTP ${response.status}).`);
  const responseBody: unknown = await response.json();
  if (!isApprovalIntent(responseBody)) throw new Error("O serviço de intenções devolveu uma resposta inválida.");
  return responseBody;
}
