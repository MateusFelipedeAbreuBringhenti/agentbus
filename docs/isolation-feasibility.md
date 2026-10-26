# Isolation Feasibility Gate — Slice #009I-C0

This disposable gate evaluates whether the approved `artifact.prepare.v1`
contract can be implemented without exposing host credentials or files. It does
not register an executor, change AgentBus authority, or enable real Tasks.

## Host capability probe

Run explicitly on the intended local host:

```bash
AGENTBUS_RUN_ISOLATION_FEASIBILITY=1 \
  pytest tests/test_isolation_feasibility.py
```

The probe uses only its pytest temporary directory and child processes. It
requires Bubblewrap and the user systemd manager. It verifies that:

- an allowlisted workspace remains writable;
- host paths are absent from the sandbox mount view;
- the child receives a cleared environment;
- the sandbox network namespace cannot reach an external address;
- the user service manager accepts the contract's CPU, memory, and PID limits;
- a child process that exceeds a smaller probe memory budget is terminated by
  the cgroup rather than allocating on the unrestricted host.

The probe does not read real user files or credentials. Sentinel names and
synthetic environment values are used instead.

## Codex SDK topology result

The installed `openai-codex` 0.158 SDK launches `codex app-server` as a child
process over stdio. Its public client accepts an approval handler, but does not
provide an external `tool_executor` or `command_handler` boundary through which
AgentBus could place every model-requested filesystem or command operation in
the independently constructed Bubblewrap sandbox.

Consequently, two distinct facts have been established:

1. The Kali host can construct the required filesystem, network, environment,
   and cgroup boundaries for a standalone child process.
2. The current SDK integration has not demonstrated that Codex tool execution
   can be routed through that child boundary while the authenticated broker
   remains outside it.

The feasibility gate is therefore **partial, not passed for production**. The
`LocalCodexExecutor` must remain absent from the operational launcher. Full
implementation requires either a supported broker/tool-executor separation in
the runtime or a different architecture reviewed by the Directorate. Running
the existing workspace-write mode is not an acceptable substitute because it
does not provide host-read confidentiality.

## Scope preserved

- no production module is changed;
- no new endpoint, permission, migration, or executor registration is added;
- no AgentBus, runner, approval, release, or Control Center behavior changes;
- no real Codex turn or plan allowance is consumed by this probe;
- project application and publication remain prohibited.
