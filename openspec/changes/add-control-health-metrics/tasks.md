## Implementation

- [x] Add leader scheduling and stable per-CR control/finalizer gauges.
- [x] Instrument requested operations and successful completion points.
- [x] Add deterministic collector replay for progress, retries, holds, first-read loss, restart and deletion retention.
- [x] Validate strict OpenSpec delta and code generation; collector/controller replay passes with race detection. Full-suite failures reproduce on unchanged dc3670d baseline and remain documented.
- [ ] Build and deploy a source-SHA-bound image with expected metrics collected.
- [ ] Complete deployed Datadog and notification replay acceptance before closing IFRBE-4086 / PMON-410.
