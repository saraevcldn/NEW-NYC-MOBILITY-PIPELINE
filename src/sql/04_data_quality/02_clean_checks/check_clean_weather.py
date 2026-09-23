from datetime import datetime

# Generate a unique ID for this DQ execution
dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Run DQ checks using Spark SQL
dq_result = spark.sql(f"""
WITH base AS (
    -- Source table being validated
    SELECT *
    FROM nyc.nyc_silver.clean_weather
),

bronze_expectations AS (
    -- Expected Silver records are based on valid and unique Bronze timestamps.
    -- This accounts for the Silver transformation removing NULL timestamps
    -- and keeping only one record per timestamp.
    SELECT COUNT(*) AS expected_silver_count
    FROM (
        SELECT timestamp
        FROM nyc.nyc_bronze.weather_bronze
        WHERE timestamp IS NOT NULL
        GROUP BY timestamp
    )
),

dq_results AS (

    -- Volume check:
    -- Compare actual Silver records against the expected valid Bronze timestamps.
    SELECT
        'clean_weather' AS table_name,
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

    -- Timestamp is the primary key and must not contain NULL values.
    SELECT
        'clean_weather',
        'Missing timestamp',
        'NULL',
        COUNT(*),
        COUNT_IF(timestamp IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Required weather measurements must not be NULL.
    SELECT
        'clean_weather',
        'Missing weather measurements',
        'NULL',
        COUNT(*),
        COUNT_IF(
            temperature_2m IS NULL
            OR precipitation IS NULL
            OR rain IS NULL
            OR snowfall IS NULL
            OR wind_speed_10m IS NULL
            OR weather_code IS NULL
        ),
        '0'
    FROM base

    UNION ALL

    -- Each hourly timestamp should appear only once.
    SELECT
        'clean_weather',
        'Duplicate timestamp primary key',
        'UNIQUE',
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT timestamp),
        '0'
    FROM base

    UNION ALL

    -- Weather measurements should not contain negative values.
    SELECT
        'clean_weather',
        'Invalid weather measurements',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            precipitation < 0
            OR rain < 0
            OR snowfall < 0
            OR wind_speed_10m < 0
        ),
        '0'
    FROM base

    UNION ALL

    -- Open-Meteo weather codes should be within the documented range.
    SELECT
        'clean_weather',
        'Invalid weather code',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            weather_code < 0
            OR weather_code > 99
        ),
        '0-99'
    FROM base

    UNION ALL

    -- Source file is required for lineage tracking.
    SELECT
        'clean_weather',
        'Missing source_file',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(source_file IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Bronze ingestion timestamp is required for lineage tracking.
    SELECT
        'clean_weather',
        'Missing bronze_ingestion_timestamp',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_ingestion_timestamp IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Bronze ingestion date is required for lineage tracking.
    SELECT
        'clean_weather',
        'Missing bronze_ingestion_date',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_ingestion_date IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Silver ingestion timestamp is required for lineage tracking.
    SELECT
        'clean_weather',
        'Missing silver_ingestion_timestamp',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(silver_ingestion_timestamp IS NULL),
        '0'
    FROM base

    UNION ALL

    -- Silver ingestion date is required for lineage tracking.
    SELECT
        'clean_weather',
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

            -- Missing timestamps are critical because timestamp is the
            -- primary key used by the Silver MERGE.
            WHEN check_name = 'Missing timestamp'
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

            -- Validity and lineage failures up to 1% are warnings.
            WHEN check_type IN (
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