# Taxi Zones Silver Data Quality Checks
#
# Silver-layer DQ checks for completeness, uniqueness, validity,
# volume reconciliation, and lineage.
# Results are evaluated as PASS, WARN, or FAIL and saved to the DQ audit table.

from datetime import datetime

dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

dq_result = spark.sql(f"""
WITH

-- Base data
base AS (
    SELECT *
    FROM nyc.nyc_silver.taxi_zones_silver
),

-- Expected volume
bronze_volume AS (
    SELECT
        COUNT(DISTINCT TRY_CAST(LocationID AS INT)) AS expected_rows
    FROM nyc.nyc_bronze.taxi_zone_bronze
    WHERE TRY_CAST(LocationID AS INT) IS NOT NULL
),

-- Valid Bronze location IDs
bronze_valid_ids AS (
    SELECT DISTINCT
        TRY_CAST(LocationID AS INT) AS location_id
    FROM nyc.nyc_bronze.taxi_zone_bronze
    WHERE TRY_CAST(LocationID AS INT) IS NOT NULL
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
        'Missing location_id' AS check_name,
        'NULL' AS check_type,
        COUNT(*) AS records_checked,
        COUNT_IF(location_id IS NULL) AS failures,
        ROUND(
            COUNT_IF(location_id IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ) AS failure_pct,
        'location_id must not be NULL' AS expected_value
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing borough',
        'NULL',
        COUNT(*),
        COUNT_IF(borough IS NULL),
        ROUND(
            COUNT_IF(borough IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'borough must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing zone',
        'NULL',
        COUNT(*),
        COUNT_IF(zone IS NULL),
        ROUND(
            COUNT_IF(zone IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'zone must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing service zone',
        'NULL',
        COUNT(*),
        COUNT_IF(service_zone IS NULL),
        ROUND(
            COUNT_IF(service_zone IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'service_zone must not be NULL'
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
        'Duplicate location_id',
        'UNIQUE',
        (SELECT COUNT(*) FROM base),
        COALESCE((
            SELECT SUM(duplicate_count - 1)
            FROM (
                SELECT
                    location_id,
                    COUNT(*) AS duplicate_count
                FROM base
                WHERE location_id IS NOT NULL
                GROUP BY location_id
                HAVING COUNT(*) > 1
            )
        ), 0),
        ROUND(
            COALESCE((
                SELECT SUM(duplicate_count - 1)
                FROM (
                    SELECT
                        location_id,
                        COUNT(*) AS duplicate_count
                    FROM base
                    WHERE location_id IS NOT NULL
                    GROUP BY location_id
                    HAVING COUNT(*) > 1
                )
            ), 0) * 100.0
            / NULLIF((SELECT COUNT(*) FROM base), 0),
            2
        ),
        'One record per location_id'

    UNION ALL

    -- Validity
    SELECT
        'Validity',
        'Invalid location_id',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            location_id IS NULL
            OR location_id <= 0
        ),
        ROUND(
            COUNT_IF(
                location_id IS NULL
                OR location_id <= 0
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Positive location_id'
    FROM base

    UNION ALL

    SELECT
        'Validity',
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
        ROUND(
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
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Valid NYC borough values'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid service zone value',
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
        ROUND(
            COUNT_IF(
                UPPER(TRIM(service_zone)) NOT IN (
                    'EWR',
                    'BORO ZONE',
                    'YELLOW ZONE',
                    'AIRPORTS',
                    'N/A'
                )
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Valid service zone values'
    FROM base

    UNION ALL

    SELECT
        'Validity',
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
        ROUND(
            COUNT_IF(
                borough <> TRIM(borough)
                OR zone <> TRIM(zone)
                OR service_zone <> TRIM(service_zone)
                OR borough = ''
                OR zone = ''
                OR service_zone = ''
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Trimmed and non-empty text values'
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
            ' valid unique Bronze location IDs'
        )
    FROM bronze_volume b
    CROSS JOIN silver_volume s

    UNION ALL

    -- Lineage
    SELECT
        'Lineage',
        'Silver location_id not found in Bronze',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(b.location_id IS NULL),
        ROUND(
            COUNT_IF(b.location_id IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'Every Silver location_id must exist in Bronze'
    FROM base s
    LEFT JOIN bronze_valid_ids b
        ON s.location_id = b.location_id

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
                'Missing location_id',
                'Missing borough',
                'Missing zone',
                'Missing service zone',
                'Missing bronze ingestion timestamp',
                'Missing bronze ingestion date',
                'Missing silver ingestion timestamp',
                'Missing silver ingestion date'
            )
            AND failures > 0
                THEN 'FAIL'

            -- Primary key uniqueness
            WHEN check_type = 'UNIQUE'
            AND failures > 0
                THEN 'FAIL'

            -- Invalid primary key
            WHEN check_name = 'Invalid location_id'
            AND failures > 0
                THEN 'FAIL'

            -- Lineage failures
            WHEN check_type = 'LINEAGE'
            AND failures > 0
                THEN 'FAIL'

            -- NULL and validity thresholds
            WHEN check_type = 'NULL'
            AND failure_pct > 1
                THEN 'FAIL'

            WHEN check_type = 'NULL'
            AND failures > 0
                THEN 'WARN'

            WHEN check_type IN (
                'VALIDITY',
                'STANDARDIZATION'
            )
            AND failure_pct > 1
                THEN 'FAIL'

            WHEN check_type IN (
                'VALIDITY',
                'STANDARDIZATION'
            )
            AND failures > 0
                THEN 'WARN'

            -- Volume threshold
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
    'taxi_zones_silver' AS table_name,
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
        WHEN 'Lineage' THEN 5
        ELSE 6
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