# DQ WARN Monitoring

## Purpose

The DQ WARN monitoring layer tracks data quality checks classified as `WARN` across the Silver datasets.

A `WARN` does not automatically stop the pipeline. Instead, the warning is persisted and monitored across DQ runs to determine whether the condition is:

* `STABLE`
* `DECREASING`
* `INCREASING`

An `INCREASING` warning indicates that the condition is getting worse and should trigger an alert.

The monitoring framework is centralized so the same process can monitor:

* `green_taxi_silver`
* `clean_weather`
* `taxi_zones_silver`

## Operational Flow

```text
                         DQ CHECKS
                             │
                 ┌───────────┼───────────┐
                 ▼           ▼           ▼
               PASS         WARN        FAIL
                 │           │           │
                 │      ┌────┴────┐      │
                 │      ▼         ▼      │
                 │   STABLE /  INCREASING│
                 │  DECREASING     │     │
                 │      │           │     │
                 └──────┘           │     │
                    │               │     │
                    ▼               ▼     ▼
                CONTINUE          ALERT  ALERT
                                    │     │
                                    ▼     ▼
                                  STOP  STOP
```

The WARN path is specifically:

```text
WARN
 ↓
Persist monitoring result
 ↓
Compare with previous run
 ↓
┌─────────────────────┐
│                     │
▼                     ▼
STABLE / DECREASING   INCREASING
│                     │
▼                     ▼
CONTINUE              ALERT
                      │
                      ▼
                     STOP
```

FAIL handling is separate from WARN monitoring. Failed records are handled through the DQ quarantine process.

## DQ Results

The existing DQ processes evaluate each Silver dataset and store their results in the centralized DQ results table:

```text
nyc.nyc_quality.dq_results
```

The DQ framework covers:

```text
green_taxi_silver
clean_weather
taxi_zones_silver
```

Each DQ result contains information such as:

* `dq_run_id`
* `dq_run_timestamp`
* `table_name`
* `category`
* `check_name`
* `check_type`
* `records_checked`
* `failures`
* `failure_pct`
* `expected_value`
* `status`

The `status` determines the operational path:

| Status | Action                       |
| ------ | ---------------------------- |
| PASS   | Continue                     |
| WARN   | Monitor                      |
| FAIL   | Alert and stop affected flow |

The existing DQ logic remains responsible for determining whether a check is `PASS`, `WARN`, or `FAIL`.

WARN monitoring does not replace the DQ checks.

## WARN Monitoring

**File:**

```text
src/sql/04_data_quality/monitor_dq_warnings.sql
```

WARN results are persisted in one centralized monitoring table:

```text
nyc.nyc_quality.dq_warn_monitoring
```

The table stores:

* `dq_run_id`
* `dq_run_timestamp`
* `table_name`
* `category`
* `check_name`
* `check_type`
* `records_checked`
* `warnings`
* `warning_pct`
* `expected_value`
* `monitored_at`

Only DQ results with:

```text
status = 'WARN'
```

are captured.

### Why use one centralized table?

A centralized table allows the same monitoring process to work across all Silver datasets.

The `table_name` column identifies which dataset produced the warning.

For example:

```text
green_taxi_silver
clean_weather
taxi_zones_silver
```

The monitoring process is therefore not hardcoded to one dataset.

If a new Silver dataset is added later and its DQ results follow the same structure, its WARN results can also be monitored without creating another monitoring table.

## Historical Monitoring

Each DQ run can produce a new monitoring record.

This allows warning conditions to be compared across multiple runs.

For example:

```text
Run 1 → 10.00%
Run 2 → 10.50%
Run 3 → 12.00%
Run 4 → 15.00%
```

The monitoring layer can identify that the warning condition is increasing over time.

This is different from simply checking whether a WARN exists.

The purpose is to identify **deterioration**.

## Trend Detection

Trend detection is handled inside:

```text
src/python/04_quality/alert_dq_warnings.py
```

The SQL used to detect increasing WARN trends is embedded directly in the Python task.

The alert logic compares the **latest WARN result** with the immediately previous result for the same dataset and DQ check.

The previous warning percentage is obtained using:

```sql
LAG(warning_pct) OVER (
    PARTITION BY table_name, check_name
    ORDER BY dq_run_timestamp
)
```

A recency rank is also assigned to each WARN result:

```sql
ROW_NUMBER() OVER (
    PARTITION BY table_name, check_name
    ORDER BY dq_run_timestamp DESC
) AS recency_rank
```

The most recent result has:

```text
recency_rank = 1
```

Only this latest result is evaluated for alerting.

This is important because the monitoring table contains historical WARN results. Without filtering to the latest result, an older WARN increase could trigger an alert even when the latest DQ run has already improved.

### Trend classification

For the latest WARN result:

```text
Current > Previous
    → INCREASING

Current < Previous
    → DECREASING

Current = Previous
    → STABLE
```

The change is also calculated in percentage points:

```text
change_in_pct_points =
current warning percentage - previous warning percentage
```

### Example

Consider the following history:

```text
Run 1 → 10.00%
Run 2 → 12.00%
Run 3 → 15.00%
Run 4 → 14.00%
```

There were increases between earlier runs:

```text
10.00% → 12.00%
12.00% → 15.00%
```

However, the latest result is:

```text
Previous: 15.00%
Latest:   14.00%

14.00 - 15.00 = -1.00 percentage point

Trend: DECREASING
```

Therefore, **no alert is triggered**.

If the latest run instead contained:

```text
Previous: 15.00%
Latest:   18.00%

18.00 - 15.00 = +3.00 percentage points

Trend: INCREASING
```

the alert task would be triggered.

This ensures that alerting reflects the **latest DQ state** rather than historical warning increases.

## Monitoring vs DQ Validation

These are separate responsibilities.

### DQ validation

Answers:

> Is the current data within the defined quality rules?

Example:

```text
Passenger count check
        ↓
15.61% affected
        ↓
WARN
```

### WARN monitoring

Answers:

> Is this warning condition getting worse compared with previous runs?

Example:

```text
Previous → 15.61%
Current  → 18.20%
             ↓
         INCREASING
             ↓
           ALERT
```

This separation prevents the monitoring layer from duplicating the DQ validation logic.

## Current Status

### Green Taxi

Current monitoring contains two DQ runs for:

```text
green_taxi_silver
```

Previous run:

```text
20260924_030424
```

Latest run:

```text
20260924_033147
```

Current WARNs:

| Check                           | Previous | Latest | Trend  |
| ------------------------------- | -------: | -----: | ------ |
| Missing or zero passenger count |   15.61% | 15.61% | STABLE |
| Extreme trip duration           |    0.40% |  0.40% | STABLE |
| Extreme trip distance           |    0.02% |  0.02% | STABLE |
| Zero-duration trip              |    0.01% |  0.01% | STABLE |

All current Green Taxi WARN conditions are stable.

### Weather

Current DQ results for:

```text
clean_weather
```

contain no WARN results.

All current checks are `PASS`.

Therefore, there are currently no Weather WARN records to monitor.

### Taxi Zones

Current DQ results for:

```text
taxi_zones_silver
```

contain no WARN results.

All current checks are `PASS`.

Therefore, there are currently no Taxi Zones WARN records to monitor.

### Current overall status

| Dataset             | PASS | WARN | FAIL | Monitoring status |
| ------------------- | ---: | ---: | ---: | ----------------- |
| `green_taxi_silver` |   28 |    4 |    0 | 4 stable WARNs    |
| `clean_weather`     |   26 |    0 |    0 | No WARNs          |
| `taxi_zones_silver` |   17 |    0 |    0 | No WARNs          |

The absence of Weather and Taxi Zones records in `dq_warn_monitoring` is expected because only `WARN` results are stored there.

## Alert Detection

**File:**

```text
src/python/04_quality/alert_dq_warnings.py
```

The Python alert task reads the WARN monitoring results and checks whether the **latest warning condition is increasing compared with the previous DQ run**.

The task:

1. Reads the centralized WARN monitoring data.
2. Calculates the previous warning percentage using `LAG()`.
3. Assigns a recency rank using `ROW_NUMBER()`.
4. Keeps only the latest WARN result using `recency_rank = 1`.
5. Compares the latest warning percentage with the previous result.
6. Identifies increasing WARN conditions.
7. Counts the detected alert conditions.
8. Raises an exception if one or more latest WARN conditions are increasing.
9. Succeeds when no increasing WARN is detected.

The alert logic therefore focuses on the **current WARN state**:

```text
Latest WARN
     ↓
Compare with previous run
     ↓
Increasing?
 ┌───────┴───────┐
 NO              YES
 ↓                ↓
Continue         ALERT
                 ↓
                STOP
```

Historical increases that are no longer present in the latest run do not trigger a new alert.

### Why this approach is used

The purpose of the alert is to identify **current deterioration**, not simply whether a warning has increased at some point in history.

For example:

```text
10% → 12% → 15% → 14%
```

Although the warning increased in earlier runs, the latest result decreased from 15% to 14%.

Therefore:

```text
Latest trend = DECREASING
Alert = NO
```

This prevents stale historical increases from repeatedly triggering alerts.

## Current Implementation Status

| Component                   | Status        | Description                                          |
| --------------------------- | ------------- | ---------------------------------------------------- |
| DQ checks                   | Implemented   | Silver datasets are evaluated using defined DQ rules |
| DQ results                  | Implemented   | Results persisted in `dq_results`                    |
| Centralized WARN monitoring | Implemented   | WARN history stored in `dq_warn_monitoring`          |
| Multi-dataset monitoring    | Implemented   | Supports Green Taxi, Weather, and Taxi Zones         |
| Historical WARN tracking    | Implemented   | Multiple DQ runs can be compared                     |
| Trend detection             | Implemented   | Stable/decreasing/increasing trends identified       |
| Increasing WARN detection   | Implemented   | Increasing warning percentage identified             |
| WARN alert Python task      | Implemented   | Task raises an exception for increasing WARNs        |
| Current WARN alert          | Not triggered | All current WARNs are stable                         |
| Databricks Job integration  | Pending       | Python alert task still needs to be added to the Job |
| Email notification          | Pending       | Job notification still needs to be configured        |

## Files

Current WARN monitoring-related files:

```text
src/
├── python/
│   └── 04_quality/
│       └── alert_dq_warnings.py
│
└── sql/
    └── 04_data_quality/
        ├── dq_run_green_taxi.sql
        ├── monitor_dq_warnings.sql
        └── ...
```

There is **no separate `alert_dq_warnings.sql` file**. The trend-detection SQL is embedded directly inside `alert_dq_warnings.py`.

### Responsibilities

| File                      | Responsibility                                        |
| ------------------------- | ----------------------------------------------------- |
| `dq_run_green_taxi.sql`   | Execute Green Taxi DQ checks                          |
| DQ SQL for Weather        | Execute Weather DQ checks                             |
| DQ SQL for Taxi Zones     | Execute Taxi Zones DQ checks                          |
| `monitor_dq_warnings.sql` | Persist WARN results from all datasets                |
| `alert_dq_warnings.py`    | Detect increasing WARN trends and fail the alert task |

## Design Principle

The monitoring framework follows this principle:

> **PASS continues. WARN is observed. Increasing WARN is escalated. FAIL is blocked.**

This allows the pipeline to distinguish between normal data-quality warnings and conditions that are becoming operationally significant.

## Next Implementation Step

The remaining implementation work is to integrate:

```text
alert_dq_warnings.py
        ↓
Databricks Job
        ↓
Task failure when WARN trend increases
        ↓
Email notification
```

The current data produces no alert because all existing WARN trends are `STABLE`.