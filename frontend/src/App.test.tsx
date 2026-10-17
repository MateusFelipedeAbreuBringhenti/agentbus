import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { App } from "./App";
import type { Task } from "./types";

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

  it("navega de Task real para detalhe e trace causal", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.includes("/correlations/")) {
        return json({ correlation_id: task.correlation_id, tasks: [task], events: [{
          id: "33333333-3333-4333-8333-333333333333", task_id: task.id, sequence: 1,
          type: "task.created", data: {}, actor_id: "orion-ops", correlation_id: task.correlation_id,
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
