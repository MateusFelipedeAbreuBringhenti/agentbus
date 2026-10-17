import { describe, expect, it, vi } from "vitest";
import { getTask, getTrace, listTasks } from "./api";

function response(value: unknown) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  }));
}

describe("AgentBus runtime contracts", () => {
  it("rejects a malformed Task", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => response({ id: "only-an-id" }));
    await expect(getTask("task-id")).rejects.toThrow("resposta inválida");
  });

  it("rejects a malformed Task page", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => response({ items: {}, next_cursor: null }));
    await expect(listTasks({})).rejects.toThrow("resposta inválida");
  });

  it("rejects malformed Events in a correlation trace", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => response({
      correlation_id: "correlation-id",
      tasks: [],
      events: [{ type: "task.created" }],
    }));
    await expect(getTrace("correlation-id")).rejects.toThrow("resposta inválida");
  });
});
