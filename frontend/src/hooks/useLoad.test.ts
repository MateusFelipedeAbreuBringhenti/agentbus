import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useLoad } from "./useLoad";

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: Error) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject };
}

describe("useLoad", () => {
  it("limpa o snapshot anterior quando a consulta seguinte falha", async () => {
    const first = deferred<string>();
    const second = deferred<string>();
    const { result, rerender } = renderHook(
      ({ request }) => useLoad(() => request.promise, [request]),
      { initialProps: { request: first } },
    );

    await act(async () => first.resolve("fresh"));
    await waitFor(() => expect(result.current.data).toBe("fresh"));

    rerender({ request: second });
    expect(result.current.data).toBeNull();
    expect(result.current.loading).toBe(true);

    await act(async () => second.reject(new Error("new request failed")));
    await waitFor(() => expect(result.current.error).toBe("new request failed"));
    expect(result.current.data).toBeNull();
    expect(result.current.loading).toBe(false);
  });

  it("ignora a conclusão de uma consulta abortada após mudança de filtro", async () => {
    const stale = deferred<string>();
    const current = deferred<string>();
    const { result, rerender } = renderHook(
      ({ request }) => useLoad(() => request.promise, [request]),
      { initialProps: { request: stale } },
    );

    rerender({ request: current });
    await act(async () => stale.resolve("stale"));
    expect(result.current.data).toBeNull();

    await act(async () => current.resolve("current"));
    await waitFor(() => expect(result.current.data).toBe("current"));
    expect(result.current.error).toBeNull();
  });
});
