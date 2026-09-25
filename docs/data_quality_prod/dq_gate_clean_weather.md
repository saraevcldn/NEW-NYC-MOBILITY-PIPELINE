# Clean Weather Data Quality Checks

## Overview

The `clean_weather` table is the Silver-layer representation of Open-Meteo weather data used to enrich the NYC mobility pipeline.

Data Quality checks are applied after the Bronze-to-Silver transformation to make sure the dataset is **complete, unique, valid, continuous, reconciled, and traceable** before it is used by downstream Gold tables and analytics.

The checks are grouped into six areas:

* **Completeness** — Are required fields populated?
* **Uniqueness** — Does each timestamp represent only one weather observation?
* **Validity** — Are measurements and field relationships within acceptable rules?
* **Volume** — Did the expected records make it from Bronze to Silver?
* **Temporal continuity** — Are hourly observations complete without unexpected gaps?
* **Lineage** — Can every record be traced to the expected source and ingestion process?

The DQ framework classifies results as **PASS, WARN, or FAIL**. Warnings are recorded while allowing processing to continue, whereas critical failures block downstream processing.

## Table Grain

The expected grain of `clean_weather` is:

> One weather observation per timestamp.

The `timestamp` column is therefore treated as the business/natural key.

## DQ Checks

### 1. Completeness

Completeness checks identify missing values.

#### Critical fields

The following fields have zero tolerance for NULL values:

* `timestamp`
* `source_file`
* `bronze_ingestion_timestamp`
* `bronze_ingestion_date`
* `silver_ingestion_timestamp`
* `silver_ingestion_date`

**Rule:**

* 0% NULL → PASS
* Any NULL → FAIL

#### Weather measurements

The following fields allow a small amount of missing data:

* `temperature_2m`
* `precipitation`
* `rain`
* `snowfall`
* `wind_speed_10m`
* `weather_code`

**Rule:**

* 0% NULL → PASS
* > 0% to ≤1% → WARN
* > 1% → FAIL

Expected NULLs should be handled according to the source semantics and should not automatically be treated as pipeline failures.

---

### 2. Uniqueness

The `timestamp` column is the natural key for the weather table.

The check detects duplicate timestamps.

**Rule:**

* 0 duplicate records → PASS
* Any duplicate → FAIL

A duplicate timestamp could cause multiple weather records to match the same taxi trip hour and potentially distort downstream joins and aggregations.

**Current result:**

* Total records: 2,208
* Unique timestamps: 2,208
* Duplicate records: 0
* Status: PASS

---

### 3. Validity

Validity checks verify that weather measurements and relationships between fields follow the expected rules.

| Check                 | Rule                    |       WARN |          FAIL |
| --------------------- | ----------------------- | ---------: | ------------: |
| Temperature           | `-50 to 50°C`           | >0% to ≤1% |           >1% |
| Precipitation         | `0–100 mm/hour`         | >0% to ≤1% |           >1% |
| Rain                  | `0–100 mm/hour`         | >0% to ≤1% |           >1% |
| Snowfall              | `0–100 cm/hour`         | >0% to ≤1% |           >1% |
| Wind speed            | `0–100 km/h`            | >0% to ≤1% |           >1% |
| Weather code          | Valid WMO code          | >0% to ≤1% |           >1% |
| Precipitation vs rain | `precipitation >= rain` | >0% to ≤1% |           >1% |
| Timestamp alignment   | Hourly timestamp grain  |          — | Any violation |

The numeric ranges are project-defined plausibility thresholds rather than official maximum limits from Open-Meteo.

For the cross-field check, `precipitation < rain` is considered invalid because total precipitation should not be less than its rain component.

The timestamp alignment check verifies that observations follow the expected hourly grain.

**Current result:** all validity checks PASS.

---

### 4. Volume Reconciliation

Volume reconciliation verifies that the Silver transformation produced the expected number of valid weather records.

The expectation is based on:

`COUNT(DISTINCT timestamp)` from Bronze where `timestamp IS NOT NULL`.

This is important because the Silver transformation uses the timestamp as the table grain and may remove invalid or duplicate records.

**Rule:**

* ≤2% difference → WARN
* > 2% difference → FAIL

For the current dataset:

```text
Bronze valid unique timestamps = 2,208
Silver records                 = 2,208
Difference                     = 0%
Status                         = PASS
```

---

### 5. Temporal Continuity

Temporal continuity verifies that the weather data does not contain unexpected gaps in the hourly sequence.

For example:

```text
01:00
02:00
03:00
05:00
```

would indicate a missing 04:00 observation.

This check is different from uniqueness because all timestamps can be unique while an observation is still missing.

**Rule:**

* No unexpected gap greater than one hour → PASS
* Any unexpected gap greater than one hour → FAIL

A simple expected-row calculation based only on `MIN(timestamp)` and `MAX(timestamp)` is avoided because the dataset uses New York local time and crosses the daylight-saving-time transition.

**Current result:**

* No gaps greater than one hour detected
* Status: PASS

---

### 6. Lineage

Lineage checks ensure that every weather record can be traced back to its source and ingestion process.

#### Source completeness

Required:

* `source_file`
* `bronze_ingestion_timestamp`
* `bronze_ingestion_date`
* `silver_ingestion_timestamp`
* `silver_ingestion_date`

Any missing critical lineage value results in FAIL.

#### Source-file validity

The source file must match the expected Open-Meteo source for the project period.

Current expected source:

`open_meteo_2026-03_to_2026-05.json`

**Rule:**

* Expected source file → PASS
* Any unexpected source file → FAIL

#### Ingestion date/timestamp consistency

The ingestion date must match the date represented by its corresponding ingestion timestamp.

```text
bronze_ingestion_date = DATE(bronze_ingestion_timestamp)

silver_ingestion_date = DATE(silver_ingestion_timestamp)
```

**Rule:**

* No mismatch → PASS
* Any mismatch → FAIL

**Current result:**

* All 2,208 records use the expected source file.
* No missing lineage values were detected.
* Lineage consistency checks pass.

---

## Current Validation Summary

| Category              | Current Result |
| --------------------- | -------------- |
| Completeness          | PASS           |
| Uniqueness            | PASS           |
| Validity              | PASS           |
| Volume reconciliation | PASS           |
| Temporal continuity   | PASS           |
| Lineage               | PASS           |

## DQ Execution Results

The `clean_weather` Data Quality task was executed against the current Silver dataset.

**Latest DQ run:**

```text
DQ Run ID:          20260924_013002
Records checked:    2,208
Total DQ checks:    26
Warnings:           0
Failures:           0
Overall status:     PASS
```

The 26 checks were distributed across the six DQ categories:

| Category              | Checks |   PASS |  WARN |  FAIL |
| --------------------- | -----: | -----: | ----: | ----: |
| Completeness          |     12 |     12 |     0 |     0 |
| Uniqueness            |      1 |      1 |     0 |     0 |
| Validity              |      8 |      8 |     0 |     0 |
| Volume Reconciliation |      1 |      1 |     0 |     0 |
| Temporal Continuity   |      1 |      1 |     0 |     0 |
| Lineage               |      3 |      3 |     0 |     0 |
| **Total**             | **26** | **26** | **0** | **0** |

### Result Details

All 2,208 Silver records passed the completeness checks, including required ingestion and lineage fields.

The uniqueness check found no duplicate timestamps.

All validity checks passed, including:

* Temperature range
* Precipitation range
* Rain range
* Snowfall range
* Wind speed range
* Weather code validity
* Precipitation versus rain consistency
* Hourly timestamp alignment

Bronze-to-Silver volume reconciliation also passed:

```text
Bronze valid unique timestamps: 2,208
Silver records:                 2,208
Difference:                     0%
```

No unexpected temporal gaps greater than one hour were detected.

All lineage checks passed, including source-file validation and ingestion date/timestamp consistency.

### DQ Gate Result

The DQ task completed successfully:

```text
26 checks executed
26 PASS
0 WARN
0 FAIL

DQ PASSED: 0 warning(s), 0 failure(s).
```

The results were persisted to:

`nyc.nyc_quality.dq_results`

The DQ task also contains a blocking gate. If a critical check returns `FAIL`, the task raises an exception, causing the downstream Databricks Job tasks that depend on it to stop.

### Current Dataset

```text
Silver records:        2,208
Unique timestamps:     2,208
Duplicate timestamps: 0
Source files:          1
Temporal gaps >1 hr:   0
Validity violations:   0
```

## DQ Gate Behavior

The DQ framework classifies results as:

```text
PASS → Continue
WARN → Continue + Record
FAIL → Stop downstream processing
```

A critical DQ failure raises an exception in the DQ task. Because downstream Databricks Job tasks depend on the DQ task, a failed DQ gate prevents downstream processing from continuing.

DQ results are persisted in:

`nyc.nyc_quality.dq_results`

This provides an audit trail of DQ runs, checks, failure counts, percentages, expected values, and statuses.

## Maintenance

Thresholds should be reviewed when:

* the Open-Meteo source changes
* the table grain changes
* new weather fields are added
* the project period changes
* source-file naming conventions change
* downstream business requirements change

Project-defined plausibility ranges should also be reviewed against observed data and source documentation before being treated as permanent business rules.