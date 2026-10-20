export type TaskStatus =
  | "ready"
  | "running"
  | "waiting_approval"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface Task {
  id: string;
  type: string;
  title: string;
  description: string | null;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  status: TaskStatus;
  requested_by: string;
  assigned_to: string | null;
  failure_code: string | null;
  failure_message: string | null;
  retry_of: string | null;
  waiting_on_approval_id: string | null;
  correlation_id: string;
  version: number;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface Event {
  id: string;
  task_id: string;
  sequence: number;
  type: string;
  data: Record<string, unknown>;
  actor_id: string;
  authenticated_principal_id: string | null;
  correlation_id: string;
  causation_event_id: string | null;
  command_id: string;
  event_index: number;
  occurred_at: string;
}

export interface TaskPage {
  items: Task[];
  next_cursor: string | null;
}

export interface CorrelationTrace {
  correlation_id: string;
  tasks: Task[];
  events: Event[];
}

export type ApprovalStatus = "pending" | "approved" | "rejected";

export interface Approval {
  id: string;
  task_id: string;
  gate: string;
  status: ApprovalStatus;
  request_reason: string;
  context: Record<string, unknown>;
  requested_by: string;
  assigned_to: string | null;
  decided_by: string | null;
  decision_reason: string | null;
  correlation_id: string;
  version: number;
  created_at: string;
  updated_at: string;
  decided_at: string | null;
}

export interface ApprovalPage { items: Approval[]; next_cursor: string | null; }

export interface ApprovalIntent {
  intent_id: string;
  approval: Pick<Approval, "id" | "task_id" | "gate" | "status" | "request_reason" | "assigned_to" | "version">;
  decision: "approve" | "reject";
  decision_reason: string | null;
  state: "pending" | "confirmed" | "sending" | "completed" | "stopped" | "expired";
  created_at: string;
  confirmed_at: string | null;
  completed_at: string | null;
  result: Approval | null;
  public_error: string | null;
  replayed: boolean | null;
}
