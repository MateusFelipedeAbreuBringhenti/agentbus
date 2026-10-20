import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { App } from "./App";
import type { Task } from "./types";
import type { Approval } from "./types";

const task: Task = {
  id: "11111111-1111-4111-8111-111111111111",
  type: "demo.real",
  title: "Task real persistida",
  description: "Prova de leitura",
  input: { a: 1 },
  output: { answer: 2 },
  status: "succeeded",
  requested_by: "orion-ops",
  assigned_to: "dex",
  failure_code: null,
  failure_message: null,
  retry_of: null,
  waiting_on_approval_id: null,
  correlation_id: "22222222-2222-4222-8222-222222222222",
  version: 3,
  created_at: "2026-10-17T12:00:00Z",
  updated_at: "2026-10-17T12:02:00Z",
  started_at: "2026-10-17T12:01:00Z",
  finished_at: "2026-10-17T12:02:00Z",
  execution_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  runner_instance_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  execution_subject_id: "dex",
  claimed_principal_id: "runner-dex-1",
  claim_policy_id: "execution-v1",
  claim_policy_sha256: "a".repeat(64),
};

function json(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } }));
}

describe("Control Center readonly", () => {
  it("distingue catálogo vazio de indisponibilidade", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => json({ items: [], next_cursor: null }));
    render(<MemoryRouter initialEntries={["/tasks"]}><App /></MemoryRouter>);
    expect(await screen.findByText("Nenhuma Task encontrada")).toBeInTheDocument();
    expect(screen.getByText(/respondeu normalmente/)).toBeInTheDocument();
  });

  it("mostra falha de rede como indisponibilidade", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("offline"));
    render(<MemoryRouter initialEntries={["/tasks"]}><App /></MemoryRouter>);
    expect(await screen.findByRole("alert")).toHaveTextContent("O AgentBus local não respondeu");
    expect(screen.queryByText("Nenhuma Task encontrada")).not.toBeInTheDocument();
  });

  it("rejeita JSON válido que viola o contrato runtime", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => json({ items: "not-an-array" }));
    render(<MemoryRouter initialEntries={["/tasks"]}><App /></MemoryRouter>);

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "O AgentBus devolveu uma resposta inválida",
    );
    expect(screen.queryByText("Nenhuma Task encontrada")).not.toBeInTheDocument();
  });

  it("navega de Task real para detalhe e trace causal", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.includes("/correlations/")) {
        return json({ correlation_id: task.correlation_id, tasks: [task], events: [{
          id: "33333333-3333-4333-8333-333333333333", task_id: task.id, sequence: 1,
          type: "task.created", data: {}, actor_id: "orion-ops",
          authenticated_principal_id: "coordinator-1", correlation_id: task.correlation_id,
          causation_event_id: null, command_id: "44444444-4444-4444-8444-444444444444",
          event_index: 0, occurred_at: task.created_at,
        }] });
      }
      if (url.includes(`/tasks/${task.id}`)) return json(task);
      return json({ items: [task], next_cursor: null });
    });
    const user = userEvent.setup();
    render(<MemoryRouter initialEntries={["/tasks"]}><App /></MemoryRouter>);
    await user.click(await screen.findByText(task.title));
    expect(await screen.findByText("Identidade e estado")).toBeInTheDocument();
    expect(screen.getByText('v3 / "v3"')).toBeInTheDocument();
    await user.click(screen.getByText(task.correlation_id));
    expect(await screen.findByText("Eventos append-only (1)")).toBeInTheDocument();
    expect(screen.getByText("task.created")).toBeInTheDocument();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
  });
});

describe("Control Center human approval intent", () => {
  it("registra intenção sem token humano e explica que não libera a Task", async () => {
    const approval: Approval = {
      id: "55555555-5555-4555-8555-555555555555", task_id: task.id,
      gate: "deployment.production", status: "pending", request_reason: "Revisar deploy",
      context: {}, requested_by: "orion", assigned_to: "mateus", decided_by: null,
      decision_reason: null, correlation_id: task.correlation_id, version: 1,
      created_at: task.created_at, updated_at: task.updated_at, decided_at: null,
    };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
      const url = String(input);
      if (url === "/intent-api/session") return json({ csrf_token: "c".repeat(43), expires_at: task.updated_at });
      if (url === "/intent-api/approval-intents") return json({
        intent_id: "66666666-6666-4666-8666-666666666666",
        approval: { id: approval.id, task_id: approval.task_id, gate: approval.gate,
          status: approval.status, request_reason: approval.request_reason,
          assigned_to: approval.assigned_to, version: approval.version },
        decision: "approve", decision_reason: null, state: "pending",
        created_at: task.created_at, confirmed_at: null, completed_at: null,
        result: null, public_error: null, replayed: null,
      }, 201);
      expect(init?.headers).toBeUndefined();
      return json({ items: [approval], next_cursor: null });
    });
    const user = userEvent.setup();
    render(<MemoryRouter initialEntries={["/approvals"]}><App /></MemoryRouter>);
    expect(await screen.findByText("Revisar deploy")).toBeInTheDocument();
    expect(screen.getByText(/não libera nem executa automaticamente/)).toBeInTheDocument();
    await user.click(screen.getByText("Solicitar aprovação"));
    await user.click(screen.getByText("Registrar intenção"));
    expect(await screen.findByText(/decisão ainda pendente/)).toBeInTheDocument();
    const serializedCalls = JSON.stringify(fetchMock.mock.calls);
    expect(serializedCalls).not.toContain("Authorization");
    expect(serializedCalls).not.toContain("Bearer");
  });

  it("mostra factualmente uma Approval aprovada que ainda aguarda release", async () => {
    const approval: Approval = {
      id: "55555555-5555-4555-8555-555555555555", task_id: task.id,
      gate: "deployment.production", status: "approved", request_reason: "Revisar deploy",
      context: {}, requested_by: "orion", assigned_to: "mateus", decided_by: "mateus",
      decision_reason: "Aprovado", correlation_id: task.correlation_id, version: 2,
      created_at: task.created_at, updated_at: task.updated_at, decided_at: task.updated_at,
    };
    const waitingTask = { ...task, status: "waiting_approval" as const,
      waiting_on_approval_id: approval.id, version: 2 };
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.includes(`/tasks/${task.id}`)) return json(waitingTask);
      return json({ items: [approval], next_cursor: null });
    });
    const user = userEvent.setup();
    render(<MemoryRouter initialEntries={["/approvals"]}><App /></MemoryRouter>);
    await user.selectOptions(await screen.findByLabelText("Status"), "approved");
    expect(await screen.findByText("Approval aprovada; Task ainda aguarda release.")).toBeInTheDocument();
    expect(screen.queryByText("Solicitar aprovação")).not.toBeInTheDocument();
  });
});

describe("Operational visibility", () => {
  it("mostra fontes indisponíveis e estado ambíguo sem oferecer recuperação", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => json({
      items: [{ operation_id: "op-safe", source_id: "release-main", kind: "release",
        persisted_state: "sending", attention_class: "active_or_ambiguous",
        recovery_mode: "exact_command_replay", task_id: task.id, approval_id: null,
        correlation_id: task.correlation_id, logical_owner: "orion-ops",
        created_at: task.created_at, state_observed_at: task.updated_at,
        timestamp_quality: "exact", last_http_status: null,
        public_error_code: "transport_error", evidence: ["journal.task_id"],
        summary: "Release ativo ou resposta ainda não confirmada." }],
      next_cursor: null, snapshot_scope: "per_request_not_global",
      sources: [{ source_id: "release-main", kind: "release", status: "available",
        observed_at: task.updated_at, public_error_code: null },
        { source_id: "runner-dex", kind: "runner", status: "unavailable",
          observed_at: task.updated_at, public_error_code: "source_unavailable" }],
    }));
    render(<MemoryRouter initialEntries={["/operations"]}><App /></MemoryRouter>);
    expect(await screen.findByText("sending")).toBeInTheDocument();
    expect(screen.getByText("runner-dex: unavailable")).toBeInTheDocument();
    expect(screen.getByText(/Idade não representa diagnóstico/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /recuper/i })).not.toBeInTheDocument();
  });
});
