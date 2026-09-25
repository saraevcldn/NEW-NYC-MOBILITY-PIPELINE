# Data Quality Gate (DQ Gate)

## Purpose

The Data Quality Gate was adjusted to make data quality checks part of the pipeline control flow, rather than only producing a report.

The DQ framework answers:

1. What should the data look like?
2. What did we actually receive?
3. Does the actual result meet the expectation?
4. Should the pipeline continue?
5. What happens when a critical check fails?

The goal is to prevent invalid or unreliable data from silently reaching downstream Gold and Analytics tables.

## DQ Framework

```text
EXPECT
   ↓
CHECK
   ↓
MEASURE
   ↓
EVALUATE
   ↓
DECIDE
   ↓
ACT
   ↓
MONITOR
```

## DQ Gate Structure

```text
                    ┌──────────────────┐
                    │   INGEST / CLEAN │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │    DQ CHECKS     │
                    │                  │
                    │ EXPECT           │
                    │ CHECK            │
                    │ MEASURE          │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │    EVALUATE      │
                    │                  │
                    │ PASS / WARN /    │
                    │ FAIL             │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
             PASS           WARN           FAIL
              │              │              │
              ▼              ▼              ▼
        ┌───────────┐  ┌─────────────┐  ┌─────────────┐
        │ Continue  │  │ Continue +  │  │ STOP        │
        │ downstream│  │ Record      │  │ downstream  │
        └───────────┘  └─────────────┘  └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │    ALERT    │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  DIAGNOSE   │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │ FIX / UPDATE│
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │    RERUN    │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │ DQ PASS     │
                                        │ → CONTINUE  │
                                        └─────────────┘
```

## What We Adjusted

### 1. Standardized the DQ framework

The checks were aligned to a common structure:

```text
EXPECT → CHECK → MEASURE → EVALUATE → DECIDE → ACT → MONITOR
```

Each check produces:

* `check_name`
* `check_type`
* `records_checked`
* `failures`
* `failure_pct`
* `expected_value`
* `status`

Each DQ execution also receives:

* `dq_run_id`
* `dq_run_timestamp`

The `dq_run_id` identifies one DQ execution so results can be traced and compared across runs.

### 2. Replaced hard-coded volume expectations

Some Silver transformations intentionally remove invalid or duplicate records.

Because of this, comparing Silver directly against the raw Bronze row count can produce a false failure.

The expected count is instead derived from Bronze using the same transformation rules:

```text
Bronze
  ↓
Apply Silver eligibility rules
  ↓
Apply natural-key deduplication
  ↓
Expected Silver count
  ↕
Actual Silver count
```

This makes the volume check dynamic and aligned with the transformation logic.

### 3. Added PASS / WARN / FAIL behavior

| Status | Meaning                                              | Pipeline behavior          |
| ------ | ---------------------------------------------------- | -------------------------- |
| PASS   | Data meets expectation                               | Continue                   |
| WARN   | Issue detected but accepted for the current use case | Continue + record          |
| FAIL   | Critical data quality issue                          | Stop downstream processing |

### 4. Made FAIL actually block the pipeline

After DQ results are generated and stored, the pipeline checks for failed checks.

```python
failed_count = dq_result.filter("status = 'FAIL'").count()

if failed_count > 0:
    raise Exception("DQ FAILED")
```

A critical DQ failure therefore prevents downstream tasks from continuing.

### 5. Added DQ audit history

DQ results are appended to:

```text
nyc.nyc_quality.dq_results
```

This allows the team to inspect:

* what was checked
* when it was checked
* how many records were affected
* what was expected
* whether the check passed, warned, or failed

## Failure and Recovery

```text
DQ FAIL
   ↓
Pipeline stops
   ↓
Failure notification
   ↓
Inspect DQ results
   ↓
Identify root cause
   ↓
Fix transformation / source issue / DQ rule
   ↓
Rerun affected stage
   ↓
Run DQ again
   ↓
DQ PASS
   ↓
Downstream processing continues
```

The important principle is that a failed DQ check is not simply logged and ignored.

The pipeline is prevented from producing downstream results from data that does not meet critical expectations.

## DQ Check Types

The framework includes:

* **NULL** — required fields are populated
* **UNIQUE** — keys are unique
* **VOLUME** — expected record counts are reconciled
* **VALIDITY** — values follow business rules
* **STANDARDIZATION** — values follow the required format
* **LINEAGE** — source and processing metadata are present

## Current DQ Scope

```text
Green Taxi Silver
    ├── Volume reconciliation
    ├── Required fields
    ├── Cleaned fields
    ├── Natural-key uniqueness
    ├── Passenger count quality
    ├── Total amount validity
    ├── Trip datetime validity
    ├── Trip distance validity
    ├── Store-and-forward standardization
    └── Lineage

Weather Silver
    ├── Volume reconciliation
    ├── Required timestamp
    ├── Weather measurements
    ├── Timestamp uniqueness
    ├── Weather value validity
    ├── Weather code validity
    └── Lineage

Taxi Zones Silver
    ├── Volume reconciliation
    ├── Required fields
    ├── Location ID uniqueness
    ├── Text standardization
    ├── Borough validity
    ├── Service-zone validity
    └── Lineage
```

## Design Principle

> **Detect early → decide explicitly → stop when necessary → recover safely → verify before continuing.**
