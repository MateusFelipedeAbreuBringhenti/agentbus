# Controlled Recovery Validation — Slice #009H-C

This slice validates existing recovery contracts. It does not add retries,
daemons, endpoints, migrations, permissions, or operational executors.

## Deterministic process interruption

`tests/test_operational_recovery_live.py` starts only child processes owned by
the test. A child reports a structured checkpoint through its stdout pipe only
after the synthetic ledger or Runner journal transaction is committed. The
harness terminates that exact `Popen` instance and never uses process-name
matching or broad signals.

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
| H4: release response lost | `test_lost_release_response_recovers_exact_command_without_duplicate_event` |
| H5: credential rotation | `test_401_suspends_exact_command_until_same_subject_reauthenticates` and subject-adversarial tests |
| H6: confirmed Approval recovery | `test_lost_success_response_recovers_exact_decision_without_releasing_task` and `test_unconfirmed_intent_is_never_recovered` |
| H7: pending coordinator outbox | `test_successor_response_loss_replays_persisted_outbox_decision` |
| H8: projection source loss | operational projection source-unavailable, replacement, WAL-preservation, and read-only tests |

H1-H3 now use child processes and a real loopback AgentBus. H4-H8 retain their
existing functional contract tests; they are not represented as multiprocess
crash validation until that remaining review requirement is implemented or the
Directorate explicitly revises its scope.

## Meaning of the result

H1 deliberately produces two `invocation_started` and two
`synthetic_effect_committed` records with one stable `execution_id`. This is
evidence of `at-least-once`, not exactly-once. H2 proves that once the result is
durable, restart reports it without invoking the executor again.

The synthetic ledger proves only the test effect. AgentBus cannot infer whether
an arbitrary external system accepted an effect before a process crash.
