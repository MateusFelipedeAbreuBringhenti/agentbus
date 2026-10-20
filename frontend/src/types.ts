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
  execution_id: string | null;
  runner_instance_id: string | null;
  execution_subject_id: string | null;
  claimed_principal_id: string | null;
  claim_policy_id: string | null;
  claim_policy_sha256: string | null;
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

export type OperationKind = "approval_intent" | "release" | "runner" | "outbox";
export interface SourceStatus { source_id: string; kind: OperationKind; status: "available" | "unavailable" | "incompatible"; observed_at: string; public_error_code: string | null; }
export interface OperationalItem {
  operation_id: string; source_id: string; kind: OperationKind; persisted_state: string;
  attention_class: string; recovery_mode: string; task_id: string | null;
  approval_id: string | null; correlation_id: string | null; logical_owner: string;
  created_at: string | null; state_observed_at: string | null;
  timestamp_quality: "exact" | "lower_bound" | "unknown"; last_http_status: number | null;
  public_error_code: string | null; evidence: string[]; summary: string;
}
export interface OperationalPage { items: OperationalItem[]; next_cursor: string | null; sources: SourceStatus[]; snapshot_scope: "per_request_not_global"; }
