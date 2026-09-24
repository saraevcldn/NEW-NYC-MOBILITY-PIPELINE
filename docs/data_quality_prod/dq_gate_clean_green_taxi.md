# Green Taxi Silver Data Quality Checks

## Overview

This document describes the Data Quality (DQ) checks implemented for the `green_taxi_silver` table.

The DQ framework validates the Silver-layer data across:

* Completeness
* Uniqueness
* Validity
* Referential integrity
* Volume reconciliation
* Lineage

The checks are evaluated as `PASS`, `WARN`, or `FAIL` and are stored in the centralized `nyc.nyc_quality.dq_results` Delta table.

The DQ design was based on profiling the actual Green Taxi data rather than applying generic rules. Profiling findings were reviewed first, then each finding was classified as either:

* a blocking data quality issue,
* a warning that should be monitored,
* an intentional transformation,
* or an issue requiring further business/source validation before becoming a blocking rule.

This approach prevents unusual but potentially legitimate records from unnecessarily stopping the pipeline.

---

## Source and Target

**Source:**

```text
nyc.nyc_bronze.green_taxi_bronze
```

**Target:**

```text
nyc.nyc_silver.green_taxi_silver
```

The Silver transformation cleans, filters, deduplicates, standardizes, and incrementally merges valid Bronze records.

The Silver table contains **128,544 records** for March through May 2026.

---

## Profiling Findings

Before implementing the DQ checks, the Green Taxi Silver data was profiled to understand its actual data characteristics.

### Null values

The profiling identified the following NULL counts:

| Column            | NULL records | Finding                              |
| ----------------- | -----------: | ------------------------------------ |
| `passenger_count` |       18,480 | Significant missing-value population |
| `ehail_fee`       |      128,544 | Entire column is NULL                |
| All other columns |            0 | No NULLs observed                    |

### Passenger count finding

`passenger_count` contained **18,480 NULL records** in the Bronze data.

Further profiling showed that these records consistently had:

* `RatecodeID` = NULL
* `payment_type` = NULL
* `trip_type` = NULL
* `store_and_fwd_flag` = NULL

The Silver transformation standardizes these values to:

```text
RatecodeID       → -1
payment_type     → -1
trip_type        → -1
store_and_fwd_flag → Unknown
```

The passenger count distribution also showed:

* `NULL` = 18,480
* `0` = 1,586
* `1` = 90,929
* `2` = 11,146
* `3` = 1,113
* `4` = 690
* `5` = 2,673
* `6` = 1,903
* `7` = 10
* `8` = 5
* `9` = 9

### Decision

The missing and zero passenger counts were **not treated as an automatic FAIL**.

The final DQ rule classifies them as:

```text
NULL or 0 → WARN
Negative → FAIL
```

No percentage threshold was applied to the missing/zero passenger check.

The reason is that profiling established that these records form a substantial and identifiable population, but did not establish that every missing or zero value represents an invalid trip.

Using a threshold such as `>1% = FAIL` would cause the DQ gate to fail every time simply because this known data characteristic exists.

Instead, the check provides visibility through a warning while reserving `FAIL` for clearly invalid negative passenger counts.

---

## Intentional Silver Transformations

Several NULL values found in Bronze are intentionally transformed during the Silver process.

| Field                  | Bronze value | Silver value | DQ treatment       |
| ---------------------- | ------------ | ------------ | ------------------ |
| `RatecodeID`           | NULL         | `-1`         | Valid Silver value |
| `payment_type`         | NULL         | `-1`         | Valid Silver value |
| `trip_type`            | NULL         | `-1`         | Valid Silver value |
| `store_and_fwd_flag`   | NULL         | `Unknown`    | Valid Silver value |
| `congestion_surcharge` | NULL         | `0.0`        | Valid Silver value |

Therefore, the DQ checks validate the **Silver-standardized values** rather than incorrectly treating these intentional transformations as data quality failures.

---

## `ehail_fee` Finding

The profiling showed:

```text
ehail_fee = NULL for 128,544 records
```

This means the entire column is NULL in the current dataset.

### Decision

This was **not added as a blocking completeness DQ check**.

The column was identified as a profiling finding, but there was insufficient evidence that NULL represents invalid data for this source and period.

Treating the entire column as a failure without confirming the source semantics would cause a known source characteristic to block the pipeline.

The finding should remain documented and can be revisited if the source specification establishes an expected value or completeness requirement for `ehail_fee`.

---

## Coded Field Profiling

The Bronze data contained the following coded values:

### `RatecodeID`

```text
NULL
1
2
3
4
5
```

Silver standardizes NULL to `-1`.

Accepted Silver values:

```text
-1, 1, 2, 3, 4, 5
```

### `payment_type`

```text
NULL
1
2
3
4
```

Silver standardizes NULL to `-1`.

Accepted Silver values:

```text
-1, 1, 2, 3, 4
```

### `trip_type`

```text
NULL
1
2
```

Silver standardizes NULL to `-1`.

Accepted Silver values:

```text
-1, 1, 2
```

### `store_and_fwd_flag`

Bronze contained:

```text
NULL
N
Y
```

Silver standardizes NULL to `Unknown`.

Accepted Silver values:

```text
N
Y
Unknown
```

### `VendorID`

Observed values:

```text
1
2
6
```

### Decision

The DQ framework checks these fields against the observed and standardized value sets.

Unexpected values are treated as `FAIL` because they indicate a value outside the expected coded domain.

---

## Trip Distance Profiling

The profiling showed:

```text
Minimum trip distance: 0.01
Maximum trip distance: 111,005.95
```

Additional profiling found:

| Condition             | Records |
| --------------------- | ------: |
| `trip_distance <= 0`  |       0 |
| `trip_distance > 50`  |      49 |
| `trip_distance > 100` |      31 |
| `trip_distance > 200` |      31 |
| `trip_distance > 500` |      31 |

The extremely high maximum value indicates that the source contains extreme distance values.

### Decision

A trip distance of `<= 0` is treated as invalid and causes `FAIL`.

A trip distance above **200 km** is treated as a warning:

```text
>200 km → WARN
```

It is not treated as an automatic failure because the profiling identified these as extreme observations, but did not establish that they are definitively invalid.

The current run contains:

```text
31 records
0.02%
WARN
```

This provides monitoring without unnecessarily blocking the pipeline.

---

## Trip Duration Profiling

Trip duration was also profiled.

| Condition             | Records |
| --------------------- | ------: |
| Duration `<= 0`       |      12 |
| Duration `> 2 hours`  |     746 |
| Duration `> 3 hours`  |     597 |
| Duration `> 6 hours`  |     515 |
| Duration `> 12 hours` |     446 |

A number of trips were unusually long, including trips approaching 24 hours.

### Decision

A negative duration is invalid:

```text
dropoff < pickup → FAIL
```

However, long durations are treated as unusual rather than automatically invalid.

The final monitoring rule is:

```text
>6 hours → WARN
```

The current run contains:

```text
515 records
0.40%
WARN
```

This threshold is a project-defined anomaly threshold for monitoring, not an official TLC maximum trip duration.

---

## Zero-Duration Trips

The profiling identified:

```text
12 trips
pickup datetime = dropoff datetime
```

These records were inspected individually.

The trips had:

* positive trip distance,
* positive fare/total amounts,
* valid location values,
* and, in some cases, missing passenger counts.

### Decision

Zero-duration trips are not automatically considered invalid.

Instead:

```text
pickup = dropoff → WARN
```

The current run contains:

```text
12 records
0.01%
WARN
```

This allows the pipeline to flag the records for review without assuming that every zero-duration record is corrupted.

---

## Financial Reconciliation Finding

The profiling tested whether:

```text
fare_amount
+ extra
+ mta_tax
+ tip_amount
+ tolls_amount
+ improvement_surcharge
+ congestion_surcharge
+ cbd_congestion_fee
= total_amount
```

The result was:

```text
25,407 mismatches
19.77% of records
Average difference: 2.97
Maximum difference: 93.96
```

The mismatch rate was too large to be explained simply by rounding.

### Decision

This was **not implemented as a blocking DQ check**.

The reason is that the exact official calculation for `total_amount` needs to be confirmed against the source/business definition before using it as a DQ rule.

Implementing an unverified financial formula as a blocking rule could incorrectly classify legitimate records as invalid.

The finding is therefore documented as a **deferred DQ enhancement**.

If the official source definition is confirmed later, the reconciliation can be added as a dedicated DQ check.

---

## Trip Key Profiling

The Silver table contains:

```text
128,544 total records
128,544 distinct trip_key values
0 NULL trip_key values
0 duplicate trip_key values
```

### Decision

`trip_key` is treated as a uniqueness requirement.

Any missing or duplicate `trip_key` results in:

```text
FAIL
```

---

## Natural Key Profiling

The source trip does not rely only on the generated Silver `trip_key`.

The transformation uses a natural trip key based on:

```text
VendorID
lpep_pickup_datetime
lpep_dropoff_datetime
PULocationID
DOLocationID
trip_distance
total_amount
```

The Silver transformation applies `ROW_NUMBER()` over this natural key and retains the most recent Bronze record.

### Decision

Natural-key duplicates are checked independently from the generated `trip_key`.

This is important because a generated identity key can be unique even when the same source trip has been ingested multiple times.

Any duplicate natural key results in:

```text
FAIL
```

The latest DQ run found:

```text
0 duplicate natural-key records
PASS
```

---

## Referential Integrity Profiling

Green Taxi pickup and dropoff locations were checked against:

```text
nyc.nyc_silver.taxi_zones_silver
```

Profiling found:

```text
Orphan PULocationID: 0
Orphan DOLocationID: 0
```

### Decision

Both pickup and dropoff location IDs must exist in the Taxi Zones dimension.

Any orphan location results in:

```text
FAIL
```

This protects downstream joins and the Gold fact table.

---

## Volume Reconciliation

The Silver transformation filters and deduplicates Bronze records before loading them.

Therefore, the expected volume is calculated using the same validity and natural-key deduplication logic applied during Silver processing.

The latest run showed:

```text
Expected valid deduplicated Bronze records: 128,544
Actual Silver records:                     128,544
Difference:                                      0
Difference %:                                  0.00%
Status:                                       PASS
```

### Decision

The volume thresholds are:

```text
0% difference        → PASS
>0% to 2%            → WARN
>2%                  → FAIL
```

This provides tolerance for small discrepancies while blocking larger reconciliation failures.

---

## Lineage Profiling

The Silver table contains Bronze source metadata:

```text
bronze_source_file
bronze_source_month
bronze_ingestion_timestamp
bronze_ingestion_date
```

and Silver processing metadata:

```text
silver_ingestion_timestamp
silver_ingestion_date
```

The following relationships were validated:

### Source month

The pickup month must match `bronze_source_month`.

```text
pickup month = bronze_source_month
```

### Bronze ingestion metadata

```text
bronze_ingestion_date
=
DATE(bronze_ingestion_timestamp)
```

### Silver ingestion metadata

```text
silver_ingestion_date
=
DATE(silver_ingestion_timestamp)
```

### Decision

Lineage mismatches are treated as `FAIL`.

These checks ensure that records can be traced back to their Bronze source and that ingestion metadata remains internally consistent.

---

## Final DQ Framework

The Green Taxi Silver DQ contains **32 checks** across six categories.

| Category              | Checks | Purpose                                                                  |
| --------------------- | -----: | ------------------------------------------------------------------------ |
| Completeness          |     10 | Detect missing critical fields and lineage metadata                      |
| Uniqueness            |      2 | Validate generated and natural trip keys                                 |
| Validity              |     14 | Validate coded fields, amounts, passenger counts, distance, and duration |
| Referential Integrity |      2 | Validate pickup/dropoff locations against Taxi Zones                     |
| Volume                |      1 | Reconcile expected Bronze and actual Silver volume                       |
| Lineage               |      3 | Validate source month and ingestion metadata                             |
| **Total**             | **32** |                                                                          |

---

## DQ Status Policy

The DQ framework uses three statuses:

### PASS

The data satisfies the expected rule.

```text
PASS → Continue pipeline
```

### WARN

The data contains an unusual or monitored condition, but the evidence does not justify blocking downstream processing.

```text
WARN → Continue pipeline + Record + Monitor
```

Examples:

* Missing/zero passenger count
* Trip distance above 200 km
* Trip duration above 6 hours
* Zero-duration trips

### FAIL

The data violates a critical structural, validity, integrity, or reconciliation rule.

```text
FAIL → Stop downstream processing + Alert
```

Examples:

* Missing critical datetime/location fields
* Duplicate trip key
* Duplicate natural key
* Negative passenger count
* Invalid coded values
* Invalid trip distance
* Dropoff before pickup
* Negative monetary values
* Orphan location IDs
* Major volume mismatch
* Lineage mismatch

---

## Latest DQ Execution Result

The latest Green Taxi Silver DQ run was:

```text
DQ Run ID:          20260924_030424
Records checked:    128,544
Total DQ checks:    32
PASS:               28
WARN:                4
FAIL:                0
Overall status:     PASS
```

### Results by category

| Category              | Checks |   PASS |  WARN |  FAIL |
| --------------------- | -----: | -----: | ----: | ----: |
| Completeness          |     10 |     10 |     0 |     0 |
| Uniqueness            |      2 |      2 |     0 |     0 |
| Validity              |     14 |     10 |     4 |     0 |
| Referential Integrity |      2 |      2 |     0 |     0 |
| Volume                |      1 |      1 |     0 |     0 |
| Lineage               |      3 |      3 |     0 |     0 |
| **Total**             | **32** | **28** | **4** | **0** |

The four warnings were:

| Check                           | Records | Failure % | Status |
| ------------------------------- | ------: | --------: | ------ |
| Missing or zero passenger count |  20,066 |    15.61% | WARN   |
| Extreme trip distance           |      31 |     0.02% | WARN   |
| Extreme trip duration           |     515 |     0.40% | WARN   |
| Zero-duration trip              |      12 |     0.01% | WARN   |

No blocking DQ failures were detected.

---

## DQ Audit Storage

Every DQ result is saved to:

```text
nyc.nyc_quality.dq_results
```

The audit table records:

```text
dq_run_id
dq_run_timestamp
table_name
check_name
check_type
records_checked
failures
failure_pct
expected_value
status
category
```

This allows DQ results to be retained across pipeline runs instead of only displaying the latest execution.

The historical DQ results can therefore be used for:

* monitoring,
* troubleshooting,
* trend analysis,
* identifying recurring data issues,
* and demonstrating that DQ checks actually ran.

---

## Failure Handling

The DQ task blocks downstream processing when one or more checks have status `FAIL`.

The execution logic is:

```text
DQ checks
    ↓
Evaluate status
    ↓
PASS / WARN / FAIL
    ↓
Save results to dq_results
    ↓
FAIL count > 0?
    ↓
YES → Raise exception → Stop downstream processing
NO  → Continue pipeline
```

Warnings do not stop the pipeline, but remain available in the audit table for monitoring.

For a blocking failure, the pipeline can then follow the recovery process:

```text
DQ FAIL
   ↓
Email alert
   ↓
Diagnose issue
   ↓
Fix transformation/source issue
   ↓
Rerun
   ↓
DQ PASS
   ↓
Continue downstream processing
```

---

## Key Design Decisions

The Green Taxi DQ framework was intentionally designed from the profiling results.

### 1. Not every unusual value is a failure

Extreme distances, long durations, and zero-duration trips are flagged as warnings instead of automatically blocking the pipeline.

### 2. Missing passenger counts are monitored

The 15.61% missing/zero passenger population is significant, but the profiling did not establish that every affected record is invalid.

Therefore:

```text
NULL/0 → WARN
negative → FAIL
```

No percentage threshold is used for this check.

### 3. Intentional Silver transformations are accepted

Values such as `-1` and `Unknown` are valid Silver representations of missing Bronze values.

The DQ checks validate the standardized Silver representation rather than treating intentional transformations as errors.

### 4. Financial reconciliation is deferred

The 19.77% financial mismatch requires confirmation of the official source calculation before it can safely become a blocking DQ rule.

### 5. Natural-key uniqueness is checked separately

The generated `trip_key` alone is not enough to detect repeated source trips.

The natural-key check provides an additional layer of protection against duplicate ingestion.

### 6. DQ is designed to support operations

The objective is not simply to report data problems.

The DQ framework is connected to pipeline behavior:

```text
Detect → Evaluate → Record → Block when necessary → Alert → Recover
```

This turns DQ from a reporting exercise into an operational control for the pipeline.

---

## Conclusion

The Green Taxi Silver DQ framework was designed based on actual profiling findings and the behavior of the Silver transformation.

The final implementation distinguishes between:

* **clearly invalid data** that should block the pipeline,
* **unusual data** that should be monitored,
* **intentional transformations** that should be accepted,
* and **findings requiring further source validation** before becoming DQ rules.

The latest execution produced:

```text
32 checks
28 PASS
4 WARN
0 FAIL
128,544 records checked
```

The pipeline therefore passed the current blocking DQ gates while retaining visibility into the four identified warning conditions.

The main lesson from the profiling process is that **good DQ is not about making every unusual value a failure**. The rules should reflect what the data actually means and whether the detected condition is serious enough to affect downstream trust.