## ADDED Requirements

### Requirement: Leader control work telemetry

The system SHALL export leader scheduling age and per-Canary control overdue state without introducing Kubernetes reads or changing release decisions. UID and revision MUST NOT become metric labels.

#### Scenario: Worker stops while metrics continue

- **WHEN** an expected job has not completed a successful live-read control call for more than ten minutes
- **THEN** its control overdue gauge is one even while the metrics endpoint responds
- **AND** a standby controller emits no control-work samples

### Requirement: Meaningful active progress

The system SHALL measure active Progressing, Promoting and Finalising steps against max(three analysis intervals, workload progress deadline), excluding explicit holds. Retry timestamps and FailedChecks MUST NOT constitute progress.

#### Scenario: Long healthy release

- **WHEN** a release lasts forty minutes and advances every five minutes
- **THEN** it is not overdue

#### Scenario: Provider retry stall

- **WHEN** an active step retries without revision, phase, weight or iteration progress beyond its budget
- **THEN** it is overdue

#### Scenario: Legal hold

- **WHEN** a Canary is paused, suspended, Waiting, WaitingPromotion, inactive or previously Failed
- **THEN** elapsed hold time does not constitute an active progress failure

### Requirement: Requested action completion

The system SHALL retain requested start, rollback and finalization clocks across retries and clear them only at successful control completion. Restarted clocks MUST NOT emit an unearned recovery zero.

#### Scenario: Rollback retry

- **WHEN** a requested rollback has not completed route, scaler, target and status work within ten minutes
- **THEN** control overdue is one until all required work succeeds

#### Scenario: Finalizer recovery

- **WHEN** finalizer removal succeeds and the CR is deleted
- **THEN** the confirmed recovery zero remains scrapeable for ten minutes before the series retires

#### Scenario: Cold active revision

- **WHEN** a restarted leader sees an unchanged active revision
- **THEN** its control state is Unknown until fresh progress or an overdue budget violation is observed
