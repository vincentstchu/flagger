## Context

The 30-day inventory includes legitimate releases lasting 34–36 minutes. A ten-minute total-release timeout is invalid. Provider retries can leave FailedChecks unchanged and refresh LastTransitionTime without progress. Current telemetry and debug-log silence cannot prove successful rollback or deletion.

## Decisions

Use a leader-only Prometheus collector attached to existing work, with no additional Kubernetes requests. Age advances at scrape time even when workers are stuck. A step advances only with revision, phase, weight or successful iteration changes. FailedChecks and retry timestamps do not reset it. Explicit suspension/pause and Waiting/WaitingPromotion are legal holds; inactive phases are not active-release failures.

The initial per-step budget is max(three analysis intervals, configured workload progress deadline), not a whole-release deadline. Job liveness and requested start/rollback/finalization have a ten-minute budget. These are conservative Slack-only starting values, not measured recovery SLAs. A five-minute Datadog evaluation is additional to these source budgets.

Start is tracked only after confirmation approves the release. Rollback clears only after routes, scaler, target and status operations succeed. Finalization clears only after finalizer removal succeeds. Retries and revision changes cannot clear pending operations.

UID and revision remain in memory. Export only stable CR name/namespace plus the bounded operation enum. A new UID resets its state; a confirmed deletion keeps recovery zeros for ten minutes before expiry. Restart/failover loses historical clocks: active unchanged or pending cleanup is Unknown until fresh progress/completion or a new budget violation, never a synthetic recovery zero. Dormant Initializing objects are deliberately outside the active-release denominator; inventory cleanup remains an operational concern.

## Migration and rollback

Build a source-SHA-bound image, deploy development then production through GitOps, verify all expected metrics and tags, and only then merge MON-03/04 Terraform. Preserve MON-01/02. Roll back the image through a reviewed GitOps PR; disable the dependent new monitors through a reviewed observability PR first. No local apply, remote-state writes or production fault injection.

## Validation boundary

Collector replay proves emitted semantics, not Datadog evaluation, Kubernetes business recovery or Slack delivery. Those require deployed monitor evidence and the bounded replay acceptance ledger. There is no new paging policy and no automatic issue closure.

The scheduler gauge is absent until the new leader actually completes scheduling. Enabling leadership does not manufacture a zero; the fixed telemetry guard owns that initial Unknown. Pending operations likewise omit healthy zeros until completion or timeout.
