# Control health metrics (PMON-410)

Leader-only gauges on the existing metrics endpoint:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `flagger_scheduler_age_seconds` | none | Seconds since scheduling completed; absent until first real completion, then ages independently of scrape reachability. |
| `flagger_canary_control_overdue` | canary_name, canary_namespace | 1 for an overdue active step, expected job or requested start/rollback; 0 after confirmed healthy observation. |
| `flagger_canary_finalizer_overdue` | canary_name, canary_namespace | 1 when requested cleanup exceeds ten minutes; clears only after finalizer removal succeeds. |
| `flagger_canary_progress_age_seconds` | canary_name, canary_namespace | Current active step age; zero during explicit hold/inactive phases. |
| `flagger_canary_progress_budget_seconds` | canary_name, canary_namespace | max(three analysis intervals, configured workload progress deadline). |
| `flagger_canary_pending_operation_age_seconds` | canary_name, canary_namespace, operation | Pending start/rollback/finalize age; zero when absent. |

Here canary_name is the **Canary CR name**, unlike legacy metrics using targetRef.name. Kubernetes cluster and controller scope come from collection tags. No UID, revision, request headers, commands or raw debug messages are exported.

Cold active state and unknown finalization omit overdue samples rather than manufacturing recovery. Whole-controller absence belongs to the fixed scheduler telemetry guard; dynamic target disappearance remains Unknown. Deletion recovery zeros expire after ten minutes. Scrape/failover continuity and group lifecycle must be verified after deployment.

Run `GOTOOLCHAIN=go1.24.0 go test -race ./pkg/metrics -run TestControlReplay` for deterministic emitted-signal replay. It does not validate Datadog evaluator behavior or notification delivery. See `openspec/changes/add-control-health-metrics/design.md` for budgets, exclusions and rollback.

A new leadership term discards prior in-memory state. Pending start/rollback/finalize emits Unknown until completion or timeout; an idle observation cannot turn an unfinished request into a recovery zero.
