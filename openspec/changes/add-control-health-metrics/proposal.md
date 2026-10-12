# Change: Export control work for PMON-410

## Why

Existing cache/status metrics remain healthy when an active control step, a requested rollback or finalizer cleanup stops completing. They cannot support MON-03/04 or deterministic acceptance.

## What Changes

- Export leader scheduling age and stable per-Canary progress, pending-operation and overdue gauges.
- Instrument existing control paths without changing their decisions, permissions or notifications.
- Replay real collector output with an injectable clock; exclude idle, suspension, confirmation waits and old Failed states.

## Impact

- Affected specs: metrics.
- Affected code: controller scheduling/finalization and metrics recorder.
- Delivery requirement: IFRBE-4085 / IFRBE-4086, PMON-410 MON-03/04.
- User implementation authorization: 2026-10-12, “MON-01～04 全部补齐，部署并回放通过后再关闭整个任务”. This authorizes preparing the implementation and deployment; acceptance remains conditional on deployed replay results. This proposal accompanies the implementation for repository review; it does not claim a merged approval or deployment.
