# Clean Weather Data Quality Checks
#
# Silver-layer DQ checks for completeness, uniqueness, validity,
# volume reconciliation, temporal continuity, and lineage.
# Results are evaluated as PASS, WARN, or FAIL and saved to the DQ audit table.

from datetime import datetime

dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

dq_result = spark.sql(f"""
WITH

-- Base data
base AS (
    SELECT *
    FROM nyc.nyc_silver.clean_weather_dlt
),

-- Expected volume from dlt Bronze
bronze_volume AS (
    SELECT
        COUNT(DISTINCT timestamp) AS expected_rows
    FROM nyc.nyc_bronze.weather_bronze_dlt
    WHERE timestamp IS NOT NULL
),

-- Actual Silver volume
silver_volume AS (
    SELECT
        COUNT(*) AS actual_rows
    FROM base
),

-- DQ checks
dq_results AS (

    -- Completeness
    SELECT
        'Completeness' AS category,
        'Missing timestamp' AS check_name,
        'NULL' AS check_type,
        COUNT(*) AS records_checked,
        COUNT_IF(timestamp IS NULL) AS failures,
        ROUND(
            COUNT_IF(timestamp IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ) AS failure_pct,
        'timestamp must not be NULL' AS expected_value
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing temperature',
        'NULL',
        COUNT(*),
        COUNT_IF(temperature_2m IS NULL),
        ROUND(
            COUNT_IF(temperature_2m IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing precipitation',
        'NULL',
        COUNT(*),
        COUNT_IF(precipitation IS NULL),
        ROUND(
            COUNT_IF(precipitation IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing rain',
        'NULL',
        COUNT(*),
        COUNT_IF(rain IS NULL),
        ROUND(
            COUNT_IF(rain IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing snowfall',
        'NULL',
        COUNT(*),
        COUNT_IF(snowfall IS NULL),
        ROUND(
            COUNT_IF(snowfall IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing wind speed',
        'NULL',
        COUNT(*),
        COUNT_IF(wind_speed_10m IS NULL),
        ROUND(
            COUNT_IF(wind_speed_10m IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing weather code',
        'NULL',
        COUNT(*),
        COUNT_IF(weather_code IS NULL),
        ROUND(
            COUNT_IF(weather_code IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'NULL <= 1%'
    FROM base

    UNION ALL

    -- Lineage completeness
    SELECT
        'Completeness',
        'Missing source file',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(source_file IS NULL),
        ROUND(
            COUNT_IF(source_file IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'source_file must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing bronze ingestion timestamp',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_ingestion_timestamp IS NULL),
        ROUND(
            COUNT_IF(bronze_ingestion_timestamp IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_ingestion_timestamp must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing bronze ingestion date',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_ingestion_date IS NULL),
        ROUND(
            COUNT_IF(bronze_ingestion_date IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_ingestion_date must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing silver ingestion timestamp',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(silver_ingestion_timestamp IS NULL),
        ROUND(
            COUNT_IF(silver_ingestion_timestamp IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'silver_ingestion_timestamp must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing silver ingestion date',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(silver_ingestion_date IS NULL),
        ROUND(
            COUNT_IF(silver_ingestion_date IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'silver_ingestion_date must not be NULL'
    FROM base

    UNION ALL

    -- Uniqueness
    SELECT
        'Uniqueness',
        'Duplicate timestamp',
        'UNIQUE',
        (SELECT COUNT(*) FROM base),
        COALESCE((
            SELECT SUM(duplicate_count - 1)
            FROM (
                SELECT
                    timestamp,
                    COUNT(*) AS duplicate_count
                FROM base
                WHERE timestamp IS NOT NULL
                GROUP BY timestamp
                HAVING COUNT(*) > 1
            )
        ), 0),
        ROUND(
            COALESCE((
                SELECT SUM(duplicate_count - 1)
                FROM (
                    SELECT
                        timestamp,
                        COUNT(*) AS duplicate_count
                    FROM base
                    WHERE timestamp IS NOT NULL
                    GROUP BY timestamp
                    HAVING COUNT(*) > 1
                )
            ), 0) * 100.0
            / NULLIF((SELECT COUNT(*) FROM base), 0),
            2
        ),
        'One weather observation per timestamp'

    UNION ALL

    -- Validity
    SELECT
        'Validity',
        'Temperature range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            temperature_2m < -50
            OR temperature_2m > 50
        ),
        ROUND(
            COUNT_IF(
                temperature_2m < -50
                OR temperature_2m > 50
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        '-50 to 50 °C'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Precipitation range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            precipitation < 0
            OR precipitation > 100
        ),
        ROUND(
            COUNT_IF(
                precipitation < 0
                OR precipitation > 100
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        '0-100 mm/hour'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Rain range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            rain < 0
            OR rain > 100
        ),
        ROUND(
            COUNT_IF(
                rain < 0
                OR rain > 100
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        '0-100 mm/hour'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Snowfall range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            snowfall < 0
            OR snowfall > 100
        ),
        ROUND(
            COUNT_IF(
                snowfall < 0
                OR snowfall > 100
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        '0-100 cm/hour'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Wind speed range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            wind_speed_10m < 0
            OR wind_speed_10m > 100
        ),
        ROUND(
            COUNT_IF(
                wind_speed_10m < 0
                OR wind_speed_10m > 100
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        '0-100 km/h'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Weather code validity',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            weather_code NOT IN (
                0, 1, 2, 3,
                45, 48,
                51, 53, 55,
                56, 57,
                61, 63, 65,
                66, 67,
                71, 73, 75, 77,
                80, 81, 82,
                85, 86,
                95, 96, 99
            )
        ),
        ROUND(
            COUNT_IF(
                weather_code NOT IN (
                    0, 1, 2, 3,
                    45, 48,
                    51, 53, 55,
                    56, 57,
                    61, 63, 65,
                    66, 67,
                    71, 73, 75, 77,
                    80, 81, 82,
                    85, 86,
                    95, 96, 99
                )
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Valid WMO weather code'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Precipitation less than rain',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(precipitation < rain),
        ROUND(
            COUNT_IF(precipitation < rain) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'precipitation >= rain'
    FROM base

    UNION ALL

    -- Timestamp grain
    SELECT
        'Validity',
        'Hourly timestamp alignment',
        'GRAIN',
        COUNT(*),
        COUNT_IF(
            timestamp IS NOT NULL
            AND date_trunc('hour', timestamp) <> timestamp
        ),
        ROUND(
            COUNT_IF(
                timestamp IS NOT NULL
                AND date_trunc('hour', timestamp) <> timestamp
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Timestamp must be aligned to the hour'
    FROM base

    UNION ALL

    -- Volume reconciliation
    SELECT
        'Volume',
        'Bronze-to-Silver valid record count',
        'VOLUME',
        s.actual_rows,
        ABS(b.expected_rows - s.actual_rows),
        ROUND(
            ABS(b.expected_rows - s.actual_rows) * 100.0
            / NULLIF(b.expected_rows, 0),
            2
        ),
        CONCAT(
            'Expected ',
            b.expected_rows,
            ' valid unique Bronze timestamps'
        )
    FROM bronze_volume b
    CROSS JOIN silver_volume s

    UNION ALL

    -- Temporal continuity
    SELECT
        'Temporal Continuity',
        'Unexpected hourly gap',
        'TEMPORAL',
        COUNT(*),
        COUNT_IF(hour_gap > 1),
        ROUND(
            COUNT_IF(hour_gap > 1) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'No unexpected gap greater than 1 hour'
    FROM (
        SELECT
            timestamp,
            TIMESTAMPDIFF(
                HOUR,
                LAG(timestamp) OVER (ORDER BY timestamp),
                timestamp
            ) AS hour_gap
        FROM base
        WHERE timestamp IS NOT NULL
    )

    UNION ALL

    -- Lineage
    SELECT
        'Lineage',
        'Unexpected source file',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            source_file IS NULL
            OR source_file NOT LIKE 'open_meteo_%'
        ),
        ROUND(
            COUNT_IF(
                source_file IS NULL
                OR source_file NOT LIKE 'open_meteo_%'
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'source_file must follow open_meteo_* naming pattern'
    FROM base

    UNION ALL

    SELECT
        'Lineage',
        'Bronze date/timestamp mismatch',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            bronze_ingestion_date
            <> CAST(bronze_ingestion_timestamp AS DATE)
        ),
        ROUND(
            COUNT_IF(
                bronze_ingestion_date
                <> CAST(bronze_ingestion_timestamp AS DATE)
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_ingestion_date = DATE(bronze_ingestion_timestamp)'
    FROM base

    UNION ALL

    SELECT
        'Lineage',
        'Silver date/timestamp mismatch',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            silver_ingestion_date
            <> CAST(silver_ingestion_timestamp AS DATE)
        ),
        ROUND(
            COUNT_IF(
                silver_ingestion_date
                <> CAST(silver_ingestion_timestamp AS DATE)
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'silver_ingestion_date = DATE(silver_ingestion_timestamp)'
    FROM base
),

-- Status evaluation
evaluated AS (
    SELECT
        *,
        CASE
            -- Critical completeness checks
            WHEN check_name IN (
                'Missing timestamp',
                'Missing source file',
                'Missing bronze ingestion timestamp',
                'Missing bronze ingestion date',
                'Missing silver ingestion timestamp',
                'Missing silver ingestion date'
            )
            AND failures > 0
                THEN 'FAIL'

            -- Uniqueness
            WHEN check_type = 'UNIQUE'
            AND failures > 0
                THEN 'FAIL'

            -- Timestamp alignment
            WHEN check_name = 'Hourly timestamp alignment'
            AND failures > 0
                THEN 'FAIL'

            -- Temporal continuity
            WHEN check_type = 'TEMPORAL'
            AND failure_pct > 1
                THEN 'FAIL'

            WHEN check_type = 'TEMPORAL'
            AND failures > 0
                THEN 'WARN'

            -- Lineage
            WHEN category = 'Lineage'
            AND failures > 0
                THEN 'FAIL'

            -- NULL checks
            WHEN check_type = 'NULL'
            AND failure_pct > 1
                THEN 'FAIL'

            WHEN check_type = 'NULL'
            AND failures > 0
                THEN 'WARN'

            -- Validity checks
            WHEN check_type = 'VALIDITY'
            AND failure_pct > 1
                THEN 'FAIL'

            WHEN check_type = 'VALIDITY'
            AND failures > 0
                THEN 'WARN'

            -- Volume
            WHEN check_type = 'VOLUME'
            AND failure_pct > 2
                THEN 'FAIL'

            WHEN check_type = 'VOLUME'
            AND failures > 0
                THEN 'WARN'

            ELSE 'PASS'
        END AS status
    FROM dq_results
)

-- Final output
SELECT
    '{dq_run_id}' AS dq_run_id,
    current_timestamp() AS dq_run_timestamp,
    category,
    'clean_weather_dlt' AS table_name,
    check_name,
    check_type,
    records_checked,
    failures,
    failure_pct,
    expected_value,
    status
FROM evaluated
ORDER BY
    CASE category
        WHEN 'Completeness' THEN 1
        WHEN 'Uniqueness' THEN 2
        WHEN 'Validity' THEN 3
        WHEN 'Volume' THEN 4
        WHEN 'Temporal Continuity' THEN 5
        WHEN 'Lineage' THEN 6
        ELSE 7
    END,
    check_name
""")

# Save DQ audit results
dq_result.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("nyc.nyc_quality.dq_results")

display(dq_result)

# Block downstream processing when DQ fails
failed_count = dq_result.filter("status = 'FAIL'").count()
warn_count = dq_result.filter("status = 'WARN'").count()

if failed_count > 0:
    raise Exception(
        f"DQ FAILED: {failed_count} check(s) failed. "
        f"{warn_count} warning(s) recorded."
    )

print(
    f"DQ PASSED: {warn_count} warning(s), "
    f"0 failure(s)."
)
