# Flagger Metrics Specification

## Requirements

### Requirement: Metrics Collection

The system SHALL collect and analyze metrics from multiple sources to evaluate the health and performance of canary
deployments.

#### Scenario: Prometheus metrics collection

- **WHEN** a canary analysis includes Prometheus-based metrics
- **THEN** the system queries Prometheus for the specified metrics
- **AND** the system validates metric values against defined thresholds
- **AND** the system records the results in the canary analysis status

#### Scenario: Custom metrics collection

- **WHEN** a canary analysis includes custom metrics defined via MetricTemplate CRDs
- **THEN** the system executes the custom metric query
- **AND** the system validates the results against thresholds
- **AND** the system handles any query execution errors appropriately

### Requirement: Metrics Recording

The system SHALL record canary analysis metrics for observability and debugging purposes.

#### Scenario: Analysis metrics recording

- **WHEN** a canary analysis is performed
- **THEN** the system records metrics about the analysis process
- **AND** the system tracks success/failure rates, timing, and other relevant KPIs
- **AND** the system makes these metrics available for external monitoring systems

### Requirement: Metrics Threshold Evaluation

The system SHALL evaluate collected metrics against defined thresholds to determine canary success or failure.

#### Scenario: Threshold validation success

- **WHEN** collected metrics meet or exceed defined thresholds
- **THEN** the system marks the analysis step as successful
- **AND** the system proceeds with the canary promotion process

#### Scenario: Threshold validation failure

- **WHEN** collected metrics fall below defined thresholds
- **THEN** the system marks the analysis step as failed
- **AND** the system initiates a rollback procedure if failure threshold is exceeded
