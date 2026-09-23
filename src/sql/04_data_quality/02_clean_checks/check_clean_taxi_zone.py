from datetime import datetime

# Generate a unique ID for this DQ execution
dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Run DQ checks using Spark SQL
dq_result = spark.sql(f"""
WITH base AS (
    -- Source table being validated
    SELECT *
    FROM nyc.nyc_silver.taxi_zones_silver
),

bronze_expectations AS (
    -- Expected Silver records are based on valid and unique Bronze location IDs.
    -- This accounts for the Silver transformation removing NULL and duplicate IDs.
    SELECT COUNT(*) AS expected_silver_count
    FROM (
        SELECT TRY_CAST(LocationID AS INT) AS location_id
        FROM nyc.nyc_bronze.taxi_zone_bronze
        WHERE TRY_CAST(LocationID AS INT) IS NOT NULL
        GROUP BY TRY_CAST(LocationID AS INT)
    )
),

dq_results AS (

    -- Volume check:
    -- Compare actual Silver records against the expected valid Bronze records.
    SELECT
        'taxi_zones_silver' AS table_name,
        'Bronze-to-Silver valid record count match' AS check_name,
        'VOLUME' AS check_type,
        COUNT(*) AS records_checked,
        CASE
            WHEN COUNT(*) = e.expected_silver_count THEN 0
            ELSE ABS(COUNT(*) - e.expected_silver_count)
        END AS failures,
        CAST(e.expected_silver_count AS STRING) AS expected_value
    FROM base
    CROSS JOIN bronze_expectations e
    GROUP BY e.expected_silver_count

    UNION ALL

    -- Primary key must not contain NULL values.
    SELECT
        'taxi_zones_silver',
        'Missing location_id',
        'NULL',
        COUNT(*),
        COUNT_IF(location_id IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Required dimension fields must not contain NULL values.
    SELECT
        'taxi_zones_silver',
        'Missing borough',
        'NULL',
        COUNT(*),
        COUNT_IF(borough IS NULL),
        '0'
    FROM base

    UNION ALL

    SELECT
        'taxi_zones_silver',
        'Missing zone',
        'NULL',
        COUNT(*),
        COUNT_IF(zone IS NULL),
        '0'
    FROM base

    UNION ALL

    SELECT
        'taxi_zones_silver',
        'Missing service_zone',
        'NULL',
        COUNT(*),
        COUNT_IF(service_zone IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Primary key must be unique.
    SELECT
        'taxi_zones_silver',
        'Duplicate location_id primary key',
        'UNIQUE',
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT location_id),
        '0'
    FROM base

    UNION ALL

    -- Text fields should have no leading/trailing whitespace or empty values.
    SELECT
        'taxi_zones_silver',
        'Unstandardized text values',
        'STANDARDIZATION',
        COUNT(*),
        COUNT_IF(
            borough <> TRIM(borough)
            OR zone <> TRIM(zone)
            OR service_zone <> TRIM(service_zone)
            OR borough = ''
            OR zone = ''
            OR service_zone = ''
        ),
        '0'
    FROM base

    UNION ALL

    -- Borough must match an accepted value.
    -- Comparison is case-insensitive and allows N/A.
    SELECT
        'taxi_zones_silver',
        'Invalid borough value',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            UPPER(TRIM(borough)) NOT IN (
                'EWR',
                'QUEENS',
                'BRONX',
                'MANHATTAN',
                'STATEN ISLAND',
                'BROOKLYN',
                'UNKNOWN',
                'N/A'
            )
        ),
        'EWR, Queens, Bronx, Manhattan, Staten Island, Brooklyn, Unknown, N/A'
    FROM base

    UNION ALL

    -- Service zone must match an accepted value.
    -- Comparison is case-insensitive and allows N/A.
    SELECT
        'taxi_zones_silver',
        'Invalid service_zone value',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            UPPER(TRIM(service_zone)) NOT IN (
                'EWR',
                'BORO ZONE',
                'YELLOW ZONE',
                'AIRPORTS',
                'N/A'
            )
        ),
        'EWR, Boro Zone, Yellow Zone, Airports, N/A'
    FROM base

    UNION ALL

    -- Silver ingestion timestamp is required for lineage tracking.
    SELECT
        'taxi_zones_silver',
        'Missing silver_ingestion_timestamp',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(silver_ingestion_timestamp IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Silver ingestion date is required for lineage tracking.
    SELECT
        'taxi_zones_silver',
        'Missing silver_ingestion_date',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(silver_ingestion_date IS NULL),
        '0'
    FROM base
),

measured AS (
    -- Calculate the percentage of records affected by each DQ failure.
    SELECT
        table_name,
        check_name,
        check_type,
        records_checked,
        failures,
        expected_value,
        ROUND(
            failures * 100.0 / NULLIF(records_checked, 0),
            2
        ) AS failure_pct
    FROM dq_results
),

evaluated AS (
    -- Convert measured failures into PASS, WARN, or FAIL
    -- based on the DQ framework thresholds.
    SELECT
        table_name,
        check_name,
        check_type,
        records_checked,
        failures,
        expected_value,
        failure_pct,

        CASE
            -- No failures means the check passes.
            WHEN failures = 0 THEN 'PASS'

            -- Any missing primary key is a critical failure.
            WHEN check_name = 'Missing location_id'
                THEN 'FAIL'

            -- Uniqueness failures up to 1% are warnings.
            WHEN check_type = 'UNIQUE'
                 AND failure_pct <= 1.0
                THEN 'WARN'

            WHEN check_type = 'UNIQUE'
                THEN 'FAIL'

            -- NULL failures up to 1% are warnings.
            WHEN check_type = 'NULL'
                 AND failure_pct <= 1.0
                THEN 'WARN'

            WHEN check_type = 'NULL'
                THEN 'FAIL'

            -- Volume differences up to 2% are warnings.
            WHEN check_type = 'VOLUME'
                 AND failure_pct <= 2.0
                THEN 'WARN'

            WHEN check_type = 'VOLUME'
                THEN 'FAIL'

            -- Standardization, validity, and lineage failures
            -- up to 1% are warnings.
            WHEN check_type IN (
                'STANDARDIZATION',
                'VALIDITY',
                'LINEAGE'
            )
            AND failure_pct <= 1.0
                THEN 'WARN'

            -- Anything above the defined thresholds fails.
            ELSE 'FAIL'
        END AS status

    FROM measured
)

SELECT
    '{dq_run_id}' AS dq_run_id,
    current_timestamp() AS dq_run_timestamp,
    table_name,
    check_name,
    check_type,
    records_checked,
    failures,
    failure_pct,
    expected_value,
    status
FROM evaluated

-- Show the most severe results first.
ORDER BY
    CASE
        WHEN status = 'FAIL' THEN 1
        WHEN status = 'WARN' THEN 2
        ELSE 3
    END,
    check_type,
    check_name
""")

# Save all DQ results for audit and historical monitoring.
(
    dq_result.write
    .mode("append")
    .format("delta")
    .saveAsTable("nyc.nyc_quality.dq_results")
)

# Display the DQ results for the current run.
display(dq_result)

# Count warnings and failures for the DQ gate.
failed_count = dq_result.filter("status = 'FAIL'").count()
warn_count = dq_result.filter("status = 'WARN'").count()

# FAIL stops the Databricks task and prevents downstream tasks from continuing.
# WARN is recorded but does not stop the pipeline.
if failed_count > 0:
    raise Exception(
        f"DQ GATE FAILED: {failed_count} check(s) failed. "
        f"{warn_count} check(s) returned WARN."
    )

print(
    f"DQ GATE PASSED: "
    f"{warn_count} check(s) returned WARN. "
    f"No FAIL results."
)