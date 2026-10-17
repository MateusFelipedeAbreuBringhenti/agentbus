import type { CorrelationTrace, Task, TaskPage, TaskStatus } from "./types";

export class AgentBusUnavailable extends Error {}

async function readJson<T>(path: string, signal?: AbortSignal): Promise<T> {
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
    return (await response.json()) as T;
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
  return readJson<TaskPage>(`/tasks?${params}`, signal);
}

export function getTask(id: string, signal?: AbortSignal): Promise<Task> {
  return readJson<Task>(`/tasks/${encodeURIComponent(id)}`, signal);
}

export function getTrace(id: string, signal?: AbortSignal): Promise<CorrelationTrace> {
  return readJson<CorrelationTrace>(`/correlations/${encodeURIComponent(id)}/trace`, signal);
}
