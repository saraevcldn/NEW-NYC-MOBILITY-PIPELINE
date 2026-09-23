# DQ Checks — Taxi Zones Silver

## Introduction

The Taxi Zones Silver DQ checks validate the cleaned and standardized NYC Taxi Zone reference data.

The checks focus on:

* Bronze-to-Silver reconciliation
* required fields
* location ID uniqueness
* text standardization
* borough validity
* service-zone validity
* lineage metadata

## Table

```text
nyc.nyc_silver.taxi_zones_silver
```

## Expected Data Behavior

The Silver transformation standardizes text values while preserving valid source records.

Because the transformation removes invalid or duplicate location IDs, the volume expectation is based on the number of **valid unique Bronze location IDs**, rather than the raw Bronze row count.

```text
Bronze taxi zones
  ↓
Valid location IDs
  ↓
Unique location IDs
  ↓
Expected Silver count
  ↕
Actual Silver count
```

## DQ Checks and Expectations

| Check                                     | Type            | Expectation                                             | Decision             |
| ----------------------------------------- | --------------- | ------------------------------------------------------- | -------------------- |
| Bronze-to-Silver valid record count match | VOLUME          | Silver count matches valid unique Bronze location count | PASS / WARN / FAIL   |
| Missing `silver_ingestion_date`           | LINEAGE         | Silver ingestion date is populated                      | PASS if complete     |
| Missing `location_id`                     | NULL            | Location ID is populated                                | PASS if complete     |
| Missing `borough`                         | NULL            | Borough is populated                                    | PASS if complete     |
| Missing `zone`                            | NULL            | Zone is populated                                       | PASS if complete     |
| Missing `service_zone`                    | NULL            | Service zone is populated                               | PASS if complete     |
| Duplicate `location_id`                   | UNIQUE          | Location ID is unique                                   | PASS if unique       |
| Unstandardized text values                | STANDARDIZATION | Text follows Silver standardization rules               | PASS if standardized |
| Invalid borough value                     | VALIDITY        | Borough belongs to approved value set                   | PASS if valid        |
| Invalid service zone value                | VALIDITY        | Service zone belongs to approved value set              | PASS if valid        |
| Missing lineage metadata                  | LINEAGE         | Required processing metadata is populated               | PASS if complete     |

## Accepted Borough Values

The DQ check evaluates values case-insensitively after trimming.

Accepted values:

```text
EWR
Queens
Bronx
Manhattan
Staten Island
Brooklyn
Unknown
N/A
```

## Accepted Service Zone Values

Accepted values:

```text
EWR
Boro Zone
Yellow Zone
Airports
N/A
```

The comparison is case-insensitive and ignores surrounding whitespace.

## Why the Validity Check Was Adjusted

Some records were initially flagged because of representation differences such as:

```text
Ewr
N/a
```

These are valid source values after standardization.

The DQ check was adjusted to evaluate the standardized value instead of treating casing differences as invalid data.

Examples:

```text
Ewr → EWR
N/a → N/A
```

## Latest Standardized DQ Result

The standardized DQ execution produced:

```text
dq_run_id: 20260923_104349

Checks: 11
PASS:   11
WARN:    0
FAIL:    0
```

The expected valid unique Bronze record count was:

```text
265
```

All defined checks passed for that execution.

## Audit

Results are written to:

```text
nyc.nyc_quality.dq_results
```

Each execution can be identified using `dq_run_id` and `dq_run_timestamp`.
