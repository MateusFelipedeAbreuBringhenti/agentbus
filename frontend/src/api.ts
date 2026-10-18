import type { CorrelationTrace, Task, TaskPage, TaskStatus } from "./types";

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
