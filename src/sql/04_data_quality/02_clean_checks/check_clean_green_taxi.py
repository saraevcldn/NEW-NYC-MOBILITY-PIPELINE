# Green Taxi Data Quality Checks
#
# Silver-layer DQ checks for completeness, uniqueness, validity,
# referential integrity, volume reconciliation, and lineage.
# Results are evaluated as PASS, WARN, or FAIL and saved to the DQ audit table.

from datetime import datetime

dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

dq_result = spark.sql(f"""
WITH

-- Base data
base AS (
    SELECT *
    FROM nyc.nyc_silver.green_taxi_silver
),

-- Expected volume
bronze_valid AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    VendorID,
                    lpep_pickup_datetime,
                    lpep_dropoff_datetime,
                    PULocationID,
                    DOLocationID,
                    trip_distance,
                    total_amount
                ORDER BY bronze_ingestion_timestamp DESC
            ) AS row_num
        FROM nyc.nyc_bronze.green_taxi_bronze
        WHERE
            lpep_pickup_datetime IS NOT NULL
            AND lpep_dropoff_datetime IS NOT NULL
            AND PULocationID IS NOT NULL
            AND DOLocationID IS NOT NULL
            AND lpep_dropoff_datetime >= lpep_pickup_datetime
            AND fare_amount >= 0
            AND total_amount >= 0
            AND trip_distance > 0
            AND lpep_pickup_datetime >= '2026-03-01'
            AND lpep_pickup_datetime < current_date()
            AND date_format(lpep_pickup_datetime, 'yyyy-MM') = source_month
    )
    WHERE row_num = 1
),

bronze_volume AS (
    SELECT COUNT(*) AS expected_rows
    FROM bronze_valid
),

-- Actual Silver volume
silver_volume AS (
    SELECT COUNT(*) AS actual_rows
    FROM base
),

-- Reference data
taxi_zones AS (
    SELECT DISTINCT
        location_id
    FROM nyc.nyc_silver.taxi_zones_silver
),

-- DQ checks
dq_results AS (

    -- Completeness
    SELECT
        'Completeness' AS category,
        'Missing pickup datetime' AS check_name,
        'NULL' AS check_type,
        COUNT(*) AS records_checked,
        COUNT_IF(lpep_pickup_datetime IS NULL) AS failures,
        ROUND(
            COUNT_IF(lpep_pickup_datetime IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ) AS failure_pct,
        'lpep_pickup_datetime must not be NULL' AS expected_value
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing dropoff datetime',
        'NULL',
        COUNT(*),
        COUNT_IF(lpep_dropoff_datetime IS NULL),
        ROUND(
            COUNT_IF(lpep_dropoff_datetime IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'lpep_dropoff_datetime must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing pickup location',
        'NULL',
        COUNT(*),
        COUNT_IF(PULocationID IS NULL),
        ROUND(
            COUNT_IF(PULocationID IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'PULocationID must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing dropoff location',
        'NULL',
        COUNT(*),
        COUNT_IF(DOLocationID IS NULL),
        ROUND(
            COUNT_IF(DOLocationID IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'DOLocationID must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing bronze source file',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_source_file IS NULL),
        ROUND(
            COUNT_IF(bronze_source_file IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_source_file must not be NULL'
    FROM base

    UNION ALL

    SELECT
        'Completeness',
        'Missing bronze source month',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(bronze_source_month IS NULL),
        ROUND(
            COUNT_IF(bronze_source_month IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_source_month must not be NULL'
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
        'Duplicate or missing trip_key',
        'UNIQUE',
        COUNT(*),
        COUNT(*) - COUNT(DISTINCT trip_key),
        ROUND(
            (COUNT(*) - COUNT(DISTINCT trip_key)) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'trip_key must be unique and not NULL'
    FROM base

    UNION ALL

    SELECT
        'Uniqueness',
        'Duplicate natural key',
        'UNIQUE',
        COUNT(*),
        COALESCE(SUM(duplicate_count - 1), 0),
        ROUND(
            COALESCE(SUM(duplicate_count - 1), 0) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'One record per natural trip key'
    FROM (
        SELECT
            VendorID,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount,
            COUNT(*) AS duplicate_count
        FROM base
        GROUP BY
            VendorID,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount
        HAVING COUNT(*) > 1
    ) duplicates
    CROSS JOIN (
        SELECT COUNT(*) AS total_rows
        FROM base
    ) totals

    UNION ALL

    -- Validity
    SELECT
        'Validity',
        'Invalid VendorID',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(VendorID NOT IN (1, 2, 6)),
        ROUND(
            COUNT_IF(VendorID NOT IN (1, 2, 6)) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'VendorID must be 1, 2, or 6'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid RatecodeID',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(RatecodeID NOT IN (-1, 1, 2, 3, 4, 5)),
        ROUND(
            COUNT_IF(RatecodeID NOT IN (-1, 1, 2, 3, 4, 5)) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'RatecodeID must be -1, 1, 2, 3, 4, or 5'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid payment_type',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(payment_type NOT IN (-1, 1, 2, 3, 4)),
        ROUND(
            COUNT_IF(payment_type NOT IN (-1, 1, 2, 3, 4)) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'payment_type must be -1, 1, 2, 3, or 4'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid trip_type',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(trip_type NOT IN (-1, 1, 2)),
        ROUND(
            COUNT_IF(trip_type NOT IN (-1, 1, 2)) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'trip_type must be -1, 1, or 2'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid store_and_fwd_flag',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            store_and_fwd_flag NOT IN ('N', 'Y', 'Unknown')
        ),
        ROUND(
            COUNT_IF(
                store_and_fwd_flag NOT IN ('N', 'Y', 'Unknown')
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'store_and_fwd_flag must be N, Y, or Unknown'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Missing or zero passenger count',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            passenger_count IS NULL
            OR passenger_count = 0
        ),
        ROUND(
            COUNT_IF(
                passenger_count IS NULL
                OR passenger_count = 0
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Passenger count should be greater than 0'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Negative passenger count',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(passenger_count < 0),
        ROUND(
            COUNT_IF(passenger_count < 0) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'Passenger count must not be negative'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid trip distance',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            trip_distance IS NULL
            OR trip_distance <= 0
        ),
        ROUND(
            COUNT_IF(
                trip_distance IS NULL
                OR trip_distance <= 0
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'trip_distance must be greater than 0'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid trip datetime range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            lpep_dropoff_datetime < lpep_pickup_datetime
        ),
        ROUND(
            COUNT_IF(
                lpep_dropoff_datetime < lpep_pickup_datetime
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'dropoff datetime must be greater than or equal to pickup datetime'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid total amount',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            total_amount IS NULL
            OR total_amount < 0
        ),
        ROUND(
            COUNT_IF(
                total_amount IS NULL
                OR total_amount < 0
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'total_amount must be zero or greater'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Invalid fare amount',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            fare_amount IS NULL
            OR fare_amount < 0
        ),
        ROUND(
            COUNT_IF(
                fare_amount IS NULL
                OR fare_amount < 0
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'fare_amount must be zero or greater'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Extreme trip distance',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(trip_distance > 200),
        ROUND(
            COUNT_IF(trip_distance > 200) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'Trip distance above 200 km requires review'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Extreme trip duration',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            lpep_dropoff_datetime - lpep_pickup_datetime
            > INTERVAL 6 HOURS
        ),
        ROUND(
            COUNT_IF(
                lpep_dropoff_datetime - lpep_pickup_datetime
                > INTERVAL 6 HOURS
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Trip duration above 6 hours requires review'
    FROM base

    UNION ALL

    SELECT
        'Validity',
        'Zero-duration trip',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            lpep_dropoff_datetime = lpep_pickup_datetime
        ),
        ROUND(
            COUNT_IF(
                lpep_dropoff_datetime = lpep_pickup_datetime
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'Zero-duration trips require review'
    FROM base

    UNION ALL

    -- Referential integrity
    SELECT
        'Referential Integrity',
        'Orphan pickup location',
        'RI',
        COUNT(*),
        COUNT_IF(t.location_id IS NULL),
        ROUND(
            COUNT_IF(t.location_id IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'Every PULocationID must exist in taxi_zones_silver'
    FROM base b
    LEFT JOIN taxi_zones t
        ON b.PULocationID = t.location_id

    UNION ALL

    SELECT
        'Referential Integrity',
        'Orphan dropoff location',
        'RI',
        COUNT(*),
        COUNT_IF(t.location_id IS NULL),
        ROUND(
            COUNT_IF(t.location_id IS NULL) * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        'Every DOLocationID must exist in taxi_zones_silver'
    FROM base b
    LEFT JOIN taxi_zones t
        ON b.DOLocationID = t.location_id

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
            ' valid deduplicated Bronze records'
        )
    FROM bronze_volume b
    CROSS JOIN silver_volume s

    UNION ALL

    -- Lineage
    SELECT
        'Lineage',
        'Source month mismatch',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            date_format(
                lpep_pickup_datetime,
                'yyyy-MM'
            ) <> bronze_source_month
        ),
        ROUND(
            COUNT_IF(
                date_format(
                    lpep_pickup_datetime,
                    'yyyy-MM'
                ) <> bronze_source_month
            ) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ),
        'bronze_source_month must match pickup month'
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
                'Missing pickup datetime',
                'Missing dropoff datetime',
                'Missing pickup location',
                'Missing dropoff location',
                'Missing bronze source file',
                'Missing bronze source month',
                'Missing bronze ingestion timestamp',
                'Missing bronze ingestion date',
                'Missing silver ingestion timestamp',
                'Missing silver ingestion date'
            )
            AND failures > 0
                THEN 'FAIL'

            -- Uniqueness checks
            WHEN check_type = 'UNIQUE'
            AND failures > 0
                THEN 'FAIL'

            -- Referential integrity checks
            WHEN check_type = 'RI'
            AND failures > 0
                THEN 'FAIL'

            -- Negative passenger count
            WHEN check_name = 'Negative passenger count'
            AND failures > 0
                THEN 'FAIL'

            -- Invalid core values
            WHEN check_name IN (
                'Invalid VendorID',
                'Invalid RatecodeID',
                'Invalid payment_type',
                'Invalid trip_type',
                'Invalid store_and_fwd_flag',
                'Invalid trip distance',
                'Invalid trip datetime range',
                'Invalid total amount',
                'Invalid fare amount'
            )
            AND failures > 0
                THEN 'FAIL'

            -- Passenger count monitoring
            WHEN check_name = 'Missing or zero passenger count'
            AND failures > 0
                THEN 'WARN'

            -- Extreme values
            WHEN check_name IN (
                'Extreme trip distance',
                'Extreme trip duration',
                'Zero-duration trip'
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

            -- Lineage failures
            WHEN check_type = 'LINEAGE'
            AND failures > 0
                THEN 'FAIL'

            ELSE 'PASS'
        END AS status
    FROM dq_results
)

-- Final output
SELECT
    '{dq_run_id}' AS dq_run_id,
    current_timestamp() AS dq_run_timestamp,
    category,
    'green_taxi_silver' AS table_name,
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
        WHEN 'Referential Integrity' THEN 4
        WHEN 'Volume' THEN 5
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
