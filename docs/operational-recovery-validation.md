# Controlled Recovery Validation — Slice #009H-C

This slice validates existing recovery contracts. It does not add retries,
daemons, endpoints, migrations, permissions, or operational executors.

## Deterministic process interruption

`tests/test_operational_recovery_live.py` starts only child processes owned by
the test. A child reports a structured checkpoint through an owned pipe only
after the relevant upstream response or local transaction is confirmed. The
harness terminates that exact subprocess and never uses process-name matching
or broad signals.

The test-only probe records `invocation_started` separately from
`synthetic_effect_committed`. Its ledger, Runner journal, and all other files
live under pytest's temporary directory. The probe is not registered in the
Runner CLI executor catalog.

## Evidence matrix

| Scenario | Automated evidence |
| --- | --- |
| H1: crash after claim/effect | `test_h1_crash_after_synthetic_effect_reexecutes_same_logical_execution` |
| H2: durable result before report | `test_h2_crash_after_durable_result_reports_without_reexecution` |
| H3: lost terminal response | live AgentBus proof in `test_h3_lost_confirmed_terminal_response_replays_without_duplicate_event` plus the established complete/fail parametrization |
| H4: release response lost | `test_h4_release_process_crash_recovers_exact_command_and_single_event` |
| H5: credential rotation | `test_h5_auth_required_process_recovers_with_new_principal_same_subject` |
| H6: confirmed Approval recovery | `test_h6_confirmed_approval_process_crash_recovers_without_new_prompt` |
| H7: pending coordinator outbox | `test_h7_orion_ops_process_crash_recovers_same_successor` |
| H8: projection source loss | `test_h8_projection_process_recovers_isolated_source_without_mutation` |

H1-H7 use owned child processes and real loopback HTTP services. H8 starts the
real projection service as an owned subprocess, makes only its isolated source
unavailable, restores that same file, and verifies recovery without modifying
the journal. Existing focused contract and adversarial tests remain additional
evidence for each component.

## Meaning of the result

H1 deliberately produces two `invocation_started` and two
`synthetic_effect_committed` records with one stable `execution_id`. This is
evidence of `at-least-once`, not exactly-once. H2 proves that once the result is
durable, restart reports it without invoking the executor again.

The synthetic ledger proves only the test effect. AgentBus cannot infer whether
an arbitrary external system accepted an effect before a process crash.

H3 discards the response in the test adapter only after the HTTP client has
received the upstream response; it is not an independent network fault proxy.
H5 obtains its `401` from the real loopback AgentBus, rejects recovery by a
different authenticated subject, and then recovers the unchanged command with
a rotated principal for the original subject. H7 captures the canonical
successor before recovery, verifies that the outbox records that same Task id,
and proves that the successor has exactly one `task.created` Event.
H6 reuses the already confirmed and persisted decision without another human
prompt. H8 proves observational recovery only and performs no canonical-state
mutation.
