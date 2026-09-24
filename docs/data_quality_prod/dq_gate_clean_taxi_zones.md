# Taxi Zones Silver Data Quality Checks

## Overview

This document defines the Data Quality (DQ) framework for the `taxi_zones_silver` table.

The DQ process validates the Silver dataset before it is used by downstream Gold-layer transformations and analytics.

The checks cover:

* Completeness
* Uniqueness
* Validity
* Volume reconciliation
* Lineage

DQ results are stored in the centralized `nyc.nyc_quality.dq_results` Delta table.

A blocking DQ gate prevents downstream processing when critical checks fail.

---

## Table Grain

The `taxi_zones_silver` table contains one record per taxi zone.

**Business key:**

```text
location_id
```

Each `location_id` must be unique and must correspond to a valid location in the Bronze source.

---

## DQ Checks

### 1. Completeness

Completeness checks verify that required fields are populated.

| Field                        | Rule             | Failure threshold |
| ---------------------------- | ---------------- | ----------------- |
| `location_id`                | Must not be NULL | Any NULL → FAIL   |
| `borough`                    | Must not be NULL | Any NULL → FAIL   |
| `zone`                       | Must not be NULL | Any NULL → FAIL   |
| `service_zone`               | Must not be NULL | Any NULL → FAIL   |
| `bronze_ingestion_timestamp` | Must not be NULL | Any NULL → FAIL   |
| `bronze_ingestion_date`      | Must not be NULL | Any NULL → FAIL   |
| `silver_ingestion_timestamp` | Must not be NULL | Any NULL → FAIL   |
| `silver_ingestion_date`      | Must not be NULL | Any NULL → FAIL   |

Required structural and ingestion metadata fields are treated as critical because missing values can affect downstream joins, lineage, or monitoring.

---

### 2. Uniqueness

The business key is `location_id`.

**Rule:**

```text
One record per location_id
```

Any duplicate `location_id` is treated as a failure.

| Result        | Status |
| ------------- | ------ |
| No duplicates | PASS   |
| Any duplicate | FAIL   |

This prevents multiple Silver records from representing the same taxi zone.

---

### 3. Validity

Validity checks verify that values conform to expected business rules.

#### Location ID

`location_id` must be a positive identifier.

```text
location_id > 0
```

Any invalid value results in a FAIL.

#### Borough

Accepted borough values are:

```text
EWR
QUEENS
BRONX
MANHATTAN
STATEN ISLAND
BROOKLYN
UNKNOWN
N/A
```

The comparison is performed after trimming whitespace and converting values to uppercase.

#### Service Zone

Accepted values are:

```text
EWR
BORO ZONE
YELLOW ZONE
AIRPORTS
N/A
```

The comparison is performed after trimming whitespace and converting values to uppercase.

#### Text Standardization

The following fields are checked for leading/trailing whitespace and empty strings:

```text
borough
zone
service_zone
```

Thresholds for validity and standardization checks:

| Failure rate | Status |
| -----------: | ------ |
|           0% | PASS   |
|   >0% to ≤1% | WARN   |
|          >1% | FAIL   |

---

### 4. Volume Reconciliation

The Silver record count is compared against the number of valid unique `LocationID` values in Bronze.

**Expected:**

```text
COUNT(DISTINCT valid Bronze LocationID)
```

**Actual:**

```text
COUNT(*) from taxi_zones_silver
```

Thresholds:

| Difference | Status |
| ---------: | ------ |
|         0% | PASS   |
| >0% to ≤2% | WARN   |
|        >2% | FAIL   |

This check detects unexpected record loss or excess records between Bronze and Silver.

---

### 5. Lineage

Lineage checks verify that Silver records can be traced back to the Bronze source.

#### Silver location ID reconciliation

Every Silver `location_id` must exist in Bronze.

```text
Silver location_id → Bronze LocationID
```

Any unmatched Silver location ID results in FAIL.

#### Bronze ingestion date consistency

```text
bronze_ingestion_date
=
DATE(bronze_ingestion_timestamp)
```

Any mismatch results in FAIL.

#### Silver ingestion date consistency

```text
silver_ingestion_date
=
DATE(silver_ingestion_timestamp)
```

Any mismatch results in FAIL.

---

## DQ Execution Results

The `taxi_zones_silver` Data Quality task was executed against the current Silver dataset.

**Latest DQ run:**

```text
DQ Run ID:          20260924_020953
Records checked:    265
Total DQ checks:    17
Warnings:           0
Failures:           0
Overall status:     PASS
```

The 17 checks were distributed across five DQ categories:

| Category              | Checks |   PASS |  WARN |  FAIL |
| --------------------- | -----: | -----: | ----: | ----: |
| Completeness          |      8 |      8 |     0 |     0 |
| Uniqueness            |      1 |      1 |     0 |     0 |
| Validity              |      4 |      4 |     0 |     0 |
| Volume Reconciliation |      1 |      1 |     0 |     0 |
| Lineage               |      3 |      3 |     0 |     0 |
| **Total**             | **17** | **17** | **0** | **0** |

All 265 records passed the defined DQ checks.

---

## Result Details

### Completeness

All eight completeness checks passed.

| Check                              | Records | Failures | Status |
| ---------------------------------- | ------: | -------: | ------ |
| Missing location_id                |     265 |        0 | PASS   |
| Missing borough                    |     265 |        0 | PASS   |
| Missing zone                       |     265 |        0 | PASS   |
| Missing service zone               |     265 |        0 | PASS   |
| Missing bronze ingestion timestamp |     265 |        0 | PASS   |
| Missing bronze ingestion date      |     265 |        0 | PASS   |
| Missing silver ingestion timestamp |     265 |        0 | PASS   |
| Missing silver ingestion date      |     265 |        0 | PASS   |

### Uniqueness

| Check                 | Records | Failures | Status |
| --------------------- | ------: | -------: | ------ |
| Duplicate location_id |     265 |        0 | PASS   |

All `location_id` values are unique.

### Validity

| Check                      | Records | Failures | Status |
| -------------------------- | ------: | -------: | ------ |
| Invalid location_id        |     265 |        0 | PASS   |
| Invalid borough value      |     265 |        0 | PASS   |
| Invalid service zone value |     265 |        0 | PASS   |
| Unstandardized text values |     265 |        0 | PASS   |

No invalid or unstandardized values were detected.

### Volume Reconciliation

| Check                               | Expected | Actual | Difference | Status |
| ----------------------------------- | -------: | -----: | ---------: | ------ |
| Bronze-to-Silver valid record count |      265 |    265 |          0 | PASS   |

The Silver record count matches the expected valid unique Bronze location IDs.

### Lineage

| Check                                  | Records | Failures | Status |
| -------------------------------------- | ------: | -------: | ------ |
| Silver location_id not found in Bronze |     265 |        0 | PASS   |
| Bronze date/timestamp mismatch         |     265 |        0 | PASS   |
| Silver date/timestamp mismatch         |     265 |        0 | PASS   |

No lineage violations were detected.

---

## DQ Gate Result

The DQ gate evaluates the final status of all checks.

```text
PASS → Continue downstream processing
WARN → Continue and record warning
FAIL → Stop downstream processing
```

For the latest execution:

```text
Warnings:  0
Failures:  0
Result:    DQ PASSED
```

The pipeline was allowed to continue because no DQ check failed.

---

## Current Dataset

The latest validation covered:

```text
Table:           nyc.nyc_silver.taxi_zones_silver
Records checked: 265
Unique zones:    265
DQ checks:       17
Warnings:        0
Failures:        0
Status:          PASS
```

---

## DQ Audit Logging

Each DQ execution is stored in:

```text
nyc.nyc_quality.dq_results
```

The audit record includes:

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

This allows DQ results to be monitored across multiple pipeline runs instead of only displaying the result of the current execution.

---

## DQ Gate Behavior

The DQ implementation raises an exception when one or more checks have a `FAIL` status.

Conceptually:

```text
DQ checks
    ↓
Evaluate status
    ↓
PASS / WARN / FAIL
    ↓
FAIL?
 ┌──┴──┐
Yes    No
 ↓      ↓
Stop   Continue
 +      +
Alert  Record
```

This makes the DQ process a real pipeline gate rather than a reporting-only step.

---

## Maintenance

The DQ rules should be reviewed when:

* The taxi zone source structure changes
* New service zone values are introduced
* The Bronze ingestion logic changes
* The Silver transformation logic changes
* New downstream dependencies are added
* Business rules for taxi zones change

Any changes to DQ rules should be tested before deployment.

---

## Summary

The `taxi_zones_silver` DQ framework provides five layers of validation:

```text
Completeness
     ↓
Uniqueness
     ↓
Validity
     ↓
Volume Reconciliation
     ↓
Lineage
     ↓
DQ Gate
```

The latest execution passed all **17 checks** across **265 records**, with **0 warnings and 0 failures**.

The dataset is currently cleared for downstream processing.