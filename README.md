# NYC Mobility Data Warehouse and Analytics Pipeline

An end-to-end data engineering project that transforms NYC taxi, location, and weather data into a structured analytical data warehouse. The project uses data from Parquet files, CSV files, and an Open API.

Built using **Databricks, Delta Lake, Unity Catalog, SQL, and Python**.

---

Project Overview

The pipeline follows a Medallion Architecture to progressively transform raw NYC mobility data into an analytical data warehouse.

``` text
Parquet Files ──────┐
                    │
CSV Files ──────────┼──► Source Inspection
                    │          │
Open API ───────────┘          ▼
                            Bronze
                              │
                              ▼
                            Silver
                              │
                              ▼
                            DQ Checks
                              │
                              ▼
                             Gold
                              │
                              ▼
                          Analytics
```

The project focuses on:

* NYC taxi trip data
* Taxi zone and location data
* Weather data
* Data cleaning and transformation
* Dimensional data modeling
* Mobility analysis

---

## Key Findings

| Business Area | Key Finding |
| :--- | :--- |
| **Taxi Demand** | • **Busiest Hours:** Taxi rides peak on weekday late afternoons from **4 PM to 6 PM**, with **Thursday at 4 PM** being the single busiest hour.<br>• **Most Common Weather:** Almost **40% of all trips** take place on **overcast (cloudy)** days, followed by clear days (25%). |
| **Weather Effect on Taxi Demand** | • **Bad Weather Spikes Demand:** Hourly demand jumps to over **70 rides/hour** during snow, compared to **44 rides/hour** on clear days.<br>• **Higher Costs & Times:** Moderate rain drives fare per mile up from **$15/mile to $27/mile**, while snow increases average trip durations. |
| **Top Areas Mobility** | • **Manhattan Leads:** Pickups and drop-offs are heavily concentrated in **Manhattan**, with major trip flows extending into **Brooklyn** and **Queens**.<br>• **Commuter Corridors:** High-demand zones maintain consistent volume across morning and evening rush hours, with longer average distances on inter-borough trips. |


### Main Takeaways
* Taxi demand reaches its highest volume during weekday late afternoons between 4 PM and 6 PM.
* Rain and snow significantly increase both hourly ride demand and average fare earnings per mile.
* Most trip activity is concentrated in Manhattan, with primary traffic flows extending into Brooklyn and Queens.
---

# Pipeline Layers

| Layer | Purpose | Key Activities |
|---|---|---|
| **Source Inspection** | Validates incoming NYC Mobility source data before ingestion | Inspects Green Taxi, Taxi Zone, and Open-Meteo sources, checks schemas, identifies missing or empty data, compares monthly Green Taxi schemas, and establishes a data-quality baseline |
| **Bronze** | Stores the raw source data in Delta tables | Preserves source structure, adds ingestion metadata, and provides the foundation for downstream processing |
| **Silver** | Creates clean and standardized datasets | Standardizes data types and categories, handles missing and invalid values, applies business rules, removes duplicates, and prepares taxi, location, and weather data for modeling |
| **Gold** | Creates the analytical data warehouse | Builds `fact_taxi_trip`, `dim_datetime`, `dim_location`, and `dim_weather_dlt`, defines the taxi trip grain, creates surrogate keys, and maintains relationships between trips and their datetime, location, and weather context |
| **Analytics** | Uses Gold data to answer NYC Mobility business questions | Analyzes taxi demand, pickup and dropoff patterns, trip duration and distance, fare activity, rush-hour patterns, location trends, and the relationship between taxi activity and weather |
---

# Gold Data Model

The Gold layer consists of **one fact table** and **three dimensions**.

## Fact Tables

| Fact Table       | Grain                 | Main Measures                                                                              |
| ---------------- | --------------------- | ------------------------------------------------------------------------------------------ |
| `fact_taxi_trip_dlt` | One row per taxi trip | `passenger_count`, `trip_distance`, `trip_duration_minutes`, `fare_amount`, `total_amount` |

## Dimension Tables

| Dimension      | Grain                        | Purpose                                       |
| -------------- | ---------------------------- | --------------------------------------------- |
| `dim_datetime` | One row per datetime         | Date and time attributes for taxi trip events |
| `dim_location` | One row per taxi zone        | NYC taxi zone and location information        |
| `dim_weather_dlt`  | One row per weather datetime | Weather conditions and measurements           |

---

# Star Schema

```text
                         dim_datetime
                              │
                              │
                              ▼
                       fact_taxi_trip_dlt
                       /           \
                      ▼             ▼
             dim_location     dim_weather_dlt
```

The star schema separates **measurable taxi trip events** in the fact table from **descriptive attributes** in the dimension tables, making the data easier to query and analyze.

---

# Datetime Design

NYC Mobility uses a shared `dim_datetime` that contains both **date and time attributes**.

Examples:

```text
2026-03-15 08:00 → Morning
2026-03-15 12:00 → Afternoon
2026-03-15 18:00 → Evening
```

The pipeline uses the shared `dim_datetime` for both pickup and dropoff timestamps.

This allows taxi trips to be compared based on **time of day, day of week, month, season, weekends, and rush hours**.

---
## dlt / Open-Meteo

In this project, **dlt refers to the open-source Python library** — not
Databricks Delta Live Tables, a different product with the same
abbreviation.

Weather ingestion runs via dlt as a GitHub Actions step rather than
inside Databricks — a deliberate workaround, not an inconsistency.
Databricks Free Edition's serverless compute blocks a network call dlt
needs; GitHub Actions has normal network access, so that one step runs
there instead. Everything downstream (`clean_weather_dlt`,
`dim_weather_dlt`) still runs as a normal Databricks task.

---
# How to Run

### Prerequisites

* Databricks workspace
* Unity Catalog access
* Databricks SQL Warehouse
* NYC Mobility datasets
* Source files available in the configured Databricks Volume
* Open-Meteo API access

### Run Order

### 1. Clone the Repository

```bash
git clone <repository-url>
cd nyc-mobility
```

### 2. Connect the Repository to Databricks

**In Databricks:**

* Open Workspace.
* Select Git folders.
* Create a new Git folder.
* Enter the GitHub repository URL.
* Select the appropriate branch.
* Create the Git folder.

### 3. Prepare the NYC Mobility Source Files

Place the source data in the configured Databricks Volume:

```text
<volume>/
├── green_taxi/
├── weather/
└── taxi_zones/
```

Green Taxi data is provided as Parquet files, Taxi Zone data as CSV, and weather data is retrieved through the Open-Meteo API.

**Execute the pipeline in the following order:**

```text
1. Setup
2. Source downloads
3. Bronze ingestion
4. Silver transformation
5. DQ Gate (validates Silver output before Gold proceeds)
6. Gold modeling
7. Analytics
```
Weather data (dlt) is ingested separately, via a GitHub Actions workflow (.github/workflows/ci-cd.yml) rather than as a step in this list — run that before step 3 so weather data is available when Silver runs.

DQ checks are automated tasks in the deployed job — a failing check blocks its downstream Gold table from running. See docs/monitoring.md and docs/runbook.md.

## Team Responsibilities

| Person | Primary ownership |
|---|---|
| Razz | Green Taxi incremental/idempotent ingestion |
| Maeve | Open-Meteo / dlt evaluation |
| Sara | DQ checks, orchestration, DQ gates, recovery |
| Yanna | CI/CD and deployment |
| Tricia | Monitoring, governance, lineage, runbook |

## Decisions

The architecture of the pipeline relies on key design decisions to guarantee reliable and scalable data processing. Below is a brief overview; for in-depth documentation, proceed to

* The pipeline is separated into Source, Bronze, Silver, Gold, and Analytics layers to isolate ingestion, transformation, modeling, and analysis.
* Delta tables provide reliable storage, allowing for safe reruns and incremental updates without duplicating data.
* The Bronze layer preserves the raw source structure, while the Silver layer handles data cleaning, type standardization, validation, and deduplication.
* Surrogate keys map facts to dimensions in the Gold layer, enforcing a strict fact grain to prevent incorrect aggregations.
* A shared `dim_datetime` is used for both pickup and dropoff timestamps, allowing analysis by hour, day of week, month, season, weekends, and rush-hour periods.
* Weather data is connected to taxi activity through datetime context so that demand and trip behavior can be analyzed alongside weather conditions.


---

## Data Validation

Validation checks are applied at every layer of the pipeline to identify issues early and ensure the final analytics are based on reliable data. Below is a brief overview; for in-depth documentation, proceed to 

* Source and Bronze layer checks verify that all expected files are present, not empty, and successfully ingested with the correct columns and row counts.
* Silver layer checks enforce data quality by verifying required fields, standardizing data types, validating numeric ranges, and removing duplicates.
* Gold layer checks validate the dimensional model by confirming dimension keys, foreign key relationships, and fact table grain.
* Analytics layer checks ensure the final output aligns with business rules, maintains the correct analytical grain, and properly handles null values.
  
---
# Documentation

Additional project documentation is available in the `docs/` directory.

| Document | Description |
|---|---|
| [`architecture.md`](docs/architecture.md) | Pipeline architecture and data flow |
| [`data-model.md`](docs/data-model.md) | Gold-layer star schema and table design |
| [`decisions.md`](docs/decisions.md) | Key technical and data-modeling decisions |
| [`governance.md`](docs/governance.md) | Ownership, lineage, and naming conventions |
| [`monitoring.md`](docs/monitoring.md) | Pipeline health checks and DQ rules |
| [`runbook.md`](docs/runbook.md) | Recovery steps when something fails |

---

# Project Outcome

The NYC Mobility Data Warehouse and Analytics Pipeline transforms raw taxi, location, and weather data into a structured analytical data warehouse using a **Medallion Architecture and Gold-layer star schema**.

The completed pipeline provides:

* A standardized process for ingesting **NYC Green Taxi, Taxi Zone, and Open-Meteo weather data**.
* A **Bronze layer** that preserves raw source data and ingestion metadata.
* A **Silver layer** that cleans, standardizes, validates, and prepares data for analytical modeling.
* A **Gold layer** containing the `fact_taxi_trip_dlt`, `dim_datetime`, `dim_location`, and `dim_weather_dlt` tables.
* Data-quality validation across the Source Inspection, Bronze, Silver, Gold, and Analytics layers.
* Time-based analysis showing how taxi demand changes by **hour and day of the week**.
* Geographic analysis showing differences in taxi activity across **NYC boroughs and locations**.
* Weather analysis comparing taxi demand and trip characteristics under different **weather and precipitation conditions**.
* A Databricks dashboard that converts the Gold-layer data into business-oriented visualizations.
