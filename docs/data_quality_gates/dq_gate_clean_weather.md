# DQ Checks — Clean Weather

## Introduction

The Clean Weather DQ checks validate the cleaned Open-Meteo weather data before it is used by downstream Gold tables.

The checks focus on:

* Bronze-to-Silver reconciliation
* timestamp completeness
* weather measurement completeness
* timestamp uniqueness
* weather measurement validity
* weather code validity
* source lineage
* Silver processing lineage

## Table

```text
nyc.nyc_silver.clean_weather
```

## Expected Data Behavior

The Silver weather transformation:

* removes records with null timestamps
* deduplicates by timestamp
* keeps the latest record for duplicate timestamps
* carries source and ingestion metadata into Silver

Therefore, the expected Silver record count is derived from Bronze using the same valid timestamp and deduplication logic.

## DQ Checks and Expectations

| Check                                     | Type     | Expectation                                                   | Decision           |
| ----------------------------------------- | -------- | ------------------------------------------------------------- | ------------------ |
| Bronze-to-Silver valid record count match | VOLUME   | Silver count matches Bronze-derived expected count            | PASS / WARN / FAIL |
| Missing timestamp                         | NULL     | Timestamp is populated                                        | PASS if complete   |
| Missing weather measurements              | NULL     | Required weather measurements are populated                   | PASS if complete   |
| Duplicate timestamp primary key           | UNIQUE   | One record per timestamp                                      | PASS if unique     |
| Invalid weather measurements              | VALIDITY | Precipitation, rain, snowfall and wind speed are not negative | PASS if valid      |
| Invalid weather code                      | VALIDITY | Weather code is within `0–99`                                 | PASS if valid      |
| Missing source file                       | LINEAGE  | Source file is populated                                      | PASS if complete   |
| Missing Bronze ingestion timestamp        | LINEAGE  | Bronze ingestion timestamp is populated                       | PASS if complete   |
| Missing Bronze ingestion date             | LINEAGE  | Bronze ingestion date is populated                            | PASS if complete   |
| Missing Silver ingestion timestamp        | LINEAGE  | Silver ingestion timestamp is populated                       | PASS if complete   |
| Missing Silver ingestion date             | LINEAGE  | Silver ingestion date is populated                            | PASS if complete   |

## Timestamp Uniqueness

Timestamp is treated as the natural key for the cleaned weather dataset.

Expected behavior:

```text
1 timestamp → 1 weather record
```

Duplicates are removed during Silver processing, and the DQ check verifies that the resulting Silver table remains unique.

## Weather Measurement Validity

The following measurements are checked for invalid negative values:

```text
precipitation
rain
snowfall
wind_speed_10m
```

Weather code is also checked against:

```text
0–99
```

## Volume Reconciliation

The volume check does not rely on a hard-coded row count.

Instead:

```text
Bronze
  ↓
Remove invalid/null timestamps
  ↓
Deduplicate timestamp
  ↓
Expected Silver count
  ↕
Actual Silver count
```

This keeps the expectation aligned with the transformation.

## Latest Standardized DQ Result

The latest standardized execution produced:

```text
dq_run_id: 20260923_105149

Checks: 11
PASS:   11
WARN:    0
FAIL:    0
```

All defined checks passed for that execution.

## Audit

Results are written to:

```text
nyc.nyc_quality.dq_results
```

Each execution can be identified using `dq_run_id` and `dq_run_timestamp`.
