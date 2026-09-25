# DQ Quarantine

## Purpose

The DQ quarantine layer stores records that fail **row-level Data Quality (DQ) checks** so they can be investigated and resolved without allowing invalid records to continue downstream.

The quarantine layer is designed to support current datasets and future ingestion sources while keeping DQ logic separate from quarantine storage.

**Flow:**

```text
Silver Data
    ↓
DQ Checks
    ↓
┌───────────┬───────────┬───────────┐
│   PASS    │   WARN    │   FAIL    │
└─────┬─────┴─────┬─────┴─────┬─────┘
      ↓           ↓           ↓
  Continue     Monitor     Quarantine
                              ↓
                            Alert
                              ↓
                             STOP
```

---

## DQ Decision Policy

| DQ Status | Action                                                            |
| --------- | ----------------------------------------------------------------- |
| PASS      | Continue downstream processing                                    |
| WARN      | Keep the record and monitor the warning                           |
| FAIL      | Quarantine affected records and stop the affected downstream flow |

This document focuses specifically on the **FAIL and quarantine process**.

---

## Architecture

The quarantine architecture separates DQ auditing from failed-record storage.

```text
                  DQ CHECKS
                      │
                      ▼
                dq_results
                      │
                     FAIL
                      │
                      ▼
              Quarantine Tables
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
     Green Taxi    Weather    Taxi Zones
     Quarantine   Quarantine  Quarantine
          │           │           │
          └───────────┼───────────┘
                      ▼
                    ALERT
                      ↓
                     STOP
```

### `dq_results`

The centralized DQ audit table records:

* DQ run ID
* Dataset/table name
* Check name
* Check category
* Check type
* Records checked
* Failure count
* Failure percentage
* Expected value
* DQ status

`dq_results` answers:

> **What DQ checks were performed and what was the result?**

### Quarantine tables

Quarantine tables store the actual records that failed applicable row-level DQ checks.

Each dataset has its own quarantine table because the source schemas are different.

Quarantine tables answer:

> **Which actual records failed, and why?**

---

## Standard Quarantine Metadata

All quarantine tables use a common metadata structure:

```text
dq_run_id
table_name
check_name
failure_reason
dq_errors
quarantined_at
is_resolved
```

### `dq_run_id`

Identifies the DQ execution that detected the failure.

This provides traceability between a quarantined record and the DQ run that identified it.

### `table_name`

Identifies the source dataset.

This allows the architecture to support multiple ingestion sources while keeping the metadata consistent.

### `check_name`

Stores the primary DQ rule associated with the failed record.

### `failure_reason`

Provides the primary reason the record was quarantined.

### `dq_errors`

Stores all row-level DQ rules that failed for the same record.

Example:

```text
[
  "Missing pickup datetime",
  "Invalid fare amount"
]
```

This prevents the same record from being inserted multiple times simply because it failed multiple DQ rules.

### `quarantined_at`

Records when the record was moved into quarantine.

### `is_resolved`

Tracks whether the quarantined record has been reviewed or resolved.

Newly quarantined records are inserted with:

```text
false
```

---

## Dataset-Specific Quarantine Tables

### Green Taxi

```text
nyc.nyc_quality.green_taxi_quarantine
```

Stores failed Green Taxi Silver records together with their DQ failure information.

The table includes the complete Green Taxi record and ingestion lineage metadata.

### Weather

```text
nyc.nyc_quality.weather_quarantine
```

Stores failed Weather Silver records.

The table includes:

* timestamp
* temperature
* precipitation
* rain
* snowfall
* wind speed
* weather code
* source file
* ingestion metadata

### Taxi Zones

```text
nyc.nyc_quality.taxi_zones_quarantine
```

Stores failed Taxi Zone Silver records.

The table includes:

* location ID
* borough
* zone
* service zone
* ingestion metadata

---

## Row-Level vs Dataset-Level Failures

Not every DQ failure can be associated with a specific bad record.

### Row-level failures

These can be quarantined because the failing record can be identified.

Examples:

```text
Missing required value
Invalid value
Invalid timestamp
Duplicate key
Invalid reference
Invalid formatting
```

### Dataset-level failures

These remain in `dq_results` and cause the pipeline to stop, but do not necessarily create quarantine records.

Examples:

```text
Volume failure
Missing expected hourly intervals
Unexpected source coverage
```

For these failures, there may be no single record that can correctly be identified as the cause.

---

## Quarantine Processing

The quarantine SQL follows the same general pattern for each dataset:

```text
1. Identify latest DQ run
        ↓
2. Identify FAIL checks
        ↓
3. Convert failed checks into flags
        ↓
4. Evaluate Silver records
        ↓
5. Build dq_errors
        ↓
6. Insert failed records into quarantine
```

The latest DQ run is used so that quarantine records correspond to the most recent DQ execution.

Only checks that actually returned `FAIL` for that DQ run are considered.

---

## Multiple DQ Failures

A record may fail more than one DQ rule.

Instead of creating multiple quarantine rows:

```text
Record 1 → Missing timestamp
Record 1 → Invalid value
```

the implementation stores one quarantine record:

```text
Record 1

dq_errors = [
    "Missing timestamp",
    "Invalid value"
]
```

This keeps quarantine records unique and makes investigation easier.

---

## Current Implementation

The quarantine tables have been prepared for:

```text
Green Taxi
Weather
Taxi Zones
```

Current SQL files:

```text
src/sql/04_data_quality/
└── quarantine_tables/
    ├── quarantine_green_taxi.sql
    ├── quarantine_weather.sql
    └── quarantine_taxi_zones.sql
```

The quarantine tables use:

```sql
CREATE TABLE IF NOT EXISTS
```

This makes the setup safe to rerun without recreating the tables.

---

## Current DQ State

At the time of implementation, the datasets do not currently contain row-level FAIL records.

### Green Taxi

```text
PASS: 28
WARN: 4
FAIL: 0
```

### Weather

```text
PASS: 26
WARN: 0
FAIL: 0
```

### Taxi Zones

```text
PASS: 17
WARN: 0
FAIL: 0
```

Therefore, the quarantine tables are currently expected to contain:

```text
0 records
```

This is expected because quarantine only receives records when applicable DQ checks return `FAIL`.

---

## Controlled Testing

The Green Taxi quarantine process was tested using a controlled DQ failure.

A test record was created with a missing pickup datetime.

The quarantine logic successfully identified:

```text
check_name:
Missing pickup datetime
```

and stored the failed rule in:

```text
dq_errors
```

The test record was then removed so that the production quarantine table remains clean.

Weather and Taxi Zone quarantine tables currently contain no records because their latest DQ runs contain no FAIL conditions.

---

## Recovery Concept

Quarantine is not intended to permanently store bad data.

The intended lifecycle is:

```text
DQ FAIL
   ↓
QUARANTINE
   ↓
INVESTIGATE
   ↓
FIX SOURCE / TRANSFORMATION
   ↓
RERUN
   ↓
DQ PASS
   ↓
CONTINUE DOWNSTREAM
   ↓
MARK QUARANTINE RECORD RESOLVED
```

The `is_resolved` field provides a place to track this lifecycle.

---

## Future-Proofing

The quarantine architecture is designed so future ingestion sources can follow the same pattern.

A new dataset only needs:

1. A dataset-specific quarantine table
2. Standard quarantine metadata
3. Row-level failure identification
4. An insertion query connected to its DQ results

The shared metadata structure keeps the quarantine process consistent even when source schemas differ.

Future datasets can therefore follow the same production pattern:

```text
INGEST
  ↓
CLEAN
  ↓
DQ
  ↓
FAIL
  ↓
QUARANTINE
  ↓
INVESTIGATE
  ↓
FIX
  ↓
RERUN
  ↓
PASS
```

---

## Files

```text
docs/
└── dq_quarantine.md

src/
└── sql/
    └── 04_data_quality/
        └── quarantine_tables/
            ├── quarantine_green_taxi.sql
            ├── quarantine_weather.sql
            └── quarantine_taxi_zones.sql
```

## Summary

The quarantine layer provides a controlled path for handling invalid records without allowing them to silently continue downstream.

The implementation provides:

* Dataset-specific quarantine tables
* Standardized quarantine metadata
* DQ run traceability
* Multiple failure reasons per record
* Separation between DQ audit results and bad-record storage
* Support for future ingestion sources
* A clear recovery lifecycle

The current datasets have no FAIL records, so the quarantine tables are intentionally empty while the architecture remains ready for future failures.