# NYC Mobility Pipeline — Governance

## 1. Purpose

This document defines the governance framework for the NYC Mobility Data
Warehouse and Analytics Pipeline: what exists, what it means, how we know
it's trustworthy, who controls it, and how data moves through it.

It is organized around five questions:

| Question | What it answers |
|---|---|
| **FIND** | Where are the data, tables, jobs, code, and docs? |
| **UNDERSTAND** | What does each table/task mean? |
| **TRUST** | How do we know the data is correct? |
| **CONTROL** | Who can access, change, or deploy things? |
| **TRACE** | Where did the data come from and how did it become the output? |

This is a living document. Sections marked **TBD** depend on work still in
progress by other pipeline owners and should be filled in as that work
lands, not guessed at now.

---

## 2. FIND — Where things live

### Unity Catalog structure

```
nyc                         (catalog)
├── nyc_bronze               raw ingested data
├── nyc_silver               cleaned/standardized data
├── nyc_gold                 star schema (fact + dimensions)
└── nyc_quality               reserved for DQ results
```

### Repository structure

```
resources/nyc_mobility_job.yml     Databricks Job / DAG definition
src/sql/00_setup/                  schema init + source download scripts
src/sql/01_bronze_ingest/          Bronze ingestion (Python/Spark)
src/sql/02_silver_clean/           Silver cleaning (SQL, MERGE INTO)
src/sql/03_gold_model/             Gold star schema (SQL, MERGE INTO)
src/sql/04_data_quality/           DQ check SQL, mirrored by layer
src/sql/05_analytics/              business-question SQL
doc/                                architecture, data model, decisions, DQ results
tests/                              pytest checks that files exist and are non-empty
.github/workflows/ci-cd.yml        validate → deploy(dev) → deploy(prod)
```

### Source systems

| Source | Format | Location |
|---|---|---|
| NYC TLC Green Taxi | Parquet | `d37ci6vzurychx.cloudfront.net/trip-data/` |
| NYC Taxi Zone Lookup | CSV | `d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` |
| Open-Meteo Historical Weather | REST API | `archive-api.open-meteo.com/v1/archive` |

All three land in a Databricks Volume (`/Volumes/nyc/default/nyc-mobility-volume/...`)
before Bronze ingestion.

---

## 3. UNDERSTAND — What the tables mean

### Gold star schema

```
                  dim_datetime
                       │
                       ▼
                fact_taxi_trip
                 /            \
                ▼              ▼
        dim_location      dim_weather
```

| Table | Grain | Business key | Surrogate key |
|---|---|---|---|
| `fact_taxi_trip` | one row per taxi trip | 7-col composite (VendorID, pickup/dropoff timestamps, PU/DOLocationID, trip_distance, total_amount) | `trip_key` |
| `dim_datetime` | one row per hour | `full_datetime` | `datetime_key` (0 = Unknown sentinel row) |
| `dim_location` | one row per taxi zone | `location_id` | `location_key` |
| `dim_weather` | one row per weather hour | `weather_datetime` | `weather_key` |

Note: Green Taxi trip data has **no natural single-column key** — the
composite key above is not even fully unique in the source (~0.5-0.7% of
rows share a key across March/April/May), which is why Silver/Gold use
`<=>` null-safe MERGE matching on the full composite rather than assuming
uniqueness. This is documented in `doc/sources/green_taxi_*.md` — treat it
as a known limitation, not a bug.

### Naming conventions (as already followed in the repo)

- **Catalog**: `nyc`
- **Schemas**: `nyc_bronze`, `nyc_silver`, `nyc_gold`, `nyc_quality`
- **Bronze tables**: `<subject>_bronze` (e.g. `green_taxi_bronze`)
- **Silver tables**: `<subject>_silver` or `clean_<subject>` (inconsistent
  today — `green_taxi_silver` / `taxi_zones_silver` vs `clean_weather`.
  **Recommendation**: standardize on `<subject>_silver` going forward and
  note the exception rather than silently renaming a live table.)
- **Gold tables**: `fact_<grain>` / `dim_<subject>`
- **Job tasks**: `<action>_<subject>` (e.g. `download_green_taxi`,
  `ingest_green_taxi`, `clean_green_taxi`)
- **DQ check files**: `check(s)_<layer_or_table>.sql`, mirrored under
  `src/sql/04_data_quality/0N_<layer>_checks/`

---

## 4. TRUST — How we know the data is correct

DQ is layered: source inspection → Bronze checks → Silver checks → Gold
(mart) checks. See `monitoring.md` for the severity policy
(PASS/WARN/FAIL) and worked examples from the actual checks in this repo.

---

## 5. CONTROL — Ownership and access

### Ownership matrix

| Area | Primary owner | Deliverable |
|---|---|---|
| Green Taxi ingestion (incremental/idempotent) | Razz | Updated ingestion code |
| Open-Meteo / dlt (open-source library) integration | Maeve | dlt pipeline + comparison |
| DQ + orchestration + recovery | Sara | DQ gates in the DAG + failure demo |
| CI/CD + deployment | Yanna | Working dev/prod deploy pipeline |
| Monitoring + governance | Tricia | This documentation set |

| Component | Backup | Escalation |
|---|---|---|
| Green Taxi | Team | - |
| Open-Meteo/dlt | Team | - |
| DQ / orchestration | Team | - |
| CI/CD | Team | - |
| Monitoring/governance | Team | - |

### Access control (proposed model — Will need to confirm)

| Resource | Developer | Reviewer | Prod deployer |
|---|---|---|---|
| GitHub repo | read/write | review | read/write |
| `develop` branch | read/write | read | read/write |
| `main` branch | PR only | review | merge |
| DEV Databricks workspace | write | write | write |
| PROD Databricks workspace | read (limited) | read | controlled write |
| GitHub Secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) | no direct access | no direct access | admin-controlled |

### Secrets

Per `.github/workflows/ci-cd.yml`, deployment credentials
(`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) are injected from GitHub Actions
Secrets at deploy time and are never committed to the repo. Governance
rule: these values must never appear in SQL/Python/YAML files, README
content, logs, or screenshots shared for the presentation.

The workflow also runs with `permissions: contents: read` at the top
level — deploy jobs only run on `push` to `develop`/`main` (not on PRs),
which limits when a workspace token is actually used.

---

## 6. TRACE — End-to-end lineage

```
NYC TLC Green Taxi Parquet
  → download_green_taxi.py → nyc.nyc_bronze.green_taxi_bronze
  → clean_green_taxi.sql   → nyc.nyc_silver.green_taxi_silver
  → fact_taxi_trip.sql     → nyc.nyc_gold.fact_taxi_trip

Open-Meteo REST API
  → download_open_meteo.py → nyc.nyc_bronze.weather_bronze
  → clean_weather.sql       → nyc.nyc_silver.clean_weather
  → dim_weather.sql         → nyc.nyc_gold.dim_weather → fact_taxi_trip (LEFT JOIN, optional)

NYC TLC Taxi Zone CSV
  → download_taxi_zones.py → nyc.nyc_bronze.taxi_zone_bronze
  → clean_taxi_zones.sql    → nyc.nyc_silver.taxi_zones_silver
  → dim_location.sql        → nyc.nyc_gold.dim_location → fact_taxi_trip (INNER JOIN, required)

fact_taxi_trip
  → taxi_demand.sql
  → weather_demand_trip_behavior.sql
  → strongest_mobility_patterns.sql
```

Lineage metadata preserved at each hop: `bronze_ingestion_timestamp`/`date`
(all sources), `silver_ingestion_timestamp`/`date` (all Silver tables),
`gold_ingestion_timestamp`/`date` or `gold_processed_timestamp`/`date`
(all Gold tables), plus `source_file`/`source_month` for Green Taxi.
