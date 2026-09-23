from datetime import datetime

# Generate a unique ID for this DQ execution
dq_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

dq_result = spark.sql(f"""
WITH base AS (
    SELECT *
    FROM nyc.nyc_silver.green_taxi_silver
),

bronze_expectations AS (
    SELECT COUNT(*) AS expected_silver_count
    FROM (
        SELECT
            VendorID,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount
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
            AND lpep_pickup_datetime < '2026-06-01'
            AND date_format(lpep_pickup_datetime, 'yyyy-MM') = source_month
        GROUP BY
            VendorID,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount
    )
),

dq_results AS (

    -- Bronze-to-Silver volume reconciliation
    SELECT
        'Bronze-to-Silver valid record count match' AS check_name,
        'VOLUME' AS check_type,
        COUNT(*) AS records_checked,
        ABS(
            COUNT(*) - (SELECT expected_silver_count FROM bronze_expectations)
        ) AS failures,
        ROUND(
            ABS(
                COUNT(*) - (SELECT expected_silver_count FROM bronze_expectations)
            ) * 100.0
            / NULLIF(
                (SELECT expected_silver_count FROM bronze_expectations),
                0
            ),
            2
        ) AS failure_pct,
        CAST(
            (SELECT expected_silver_count FROM bronze_expectations)
            AS STRING
        ) AS expected_value
    FROM base

    UNION ALL

    -- Required Bronze lineage fields
    SELECT
        'Missing Bronze lineage metadata',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            bronze_source_file IS NULL
            OR bronze_source_month IS NULL
            OR bronze_ingestion_timestamp IS NULL
            OR bronze_ingestion_date IS NULL
        ),
        ROUND(
            COUNT_IF(
                bronze_source_file IS NULL
                OR bronze_source_month IS NULL
                OR bronze_ingestion_timestamp IS NULL
                OR bronze_ingestion_date IS NULL
            ) * 100.0 / COUNT(*),
            2
        ),
        'All Bronze lineage fields populated'
    FROM base

    UNION ALL

    -- Required Silver lineage fields
    SELECT
        'Missing Silver lineage metadata',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            silver_ingestion_timestamp IS NULL
            OR silver_ingestion_date IS NULL
        ),
        ROUND(
            COUNT_IF(
                silver_ingestion_timestamp IS NULL
                OR silver_ingestion_date IS NULL
            ) * 100.0 / COUNT(*),
            2
        ),
        'All Silver lineage fields populated'
    FROM base

    UNION ALL

    -- Source month should match pickup month
    SELECT
        'Source month mismatch',
        'LINEAGE',
        COUNT(*),
        COUNT_IF(
            source_month IS NULL
            OR date_format(lpep_pickup_datetime, 'yyyy-MM') <> source_month
        ),
        ROUND(
            COUNT_IF(
                source_month IS NULL
                OR date_format(lpep_pickup_datetime, 'yyyy-MM') <> source_month
            ) * 100.0 / COUNT(*),
            2
        ),
        'source_month matches pickup month'
    FROM (
        SELECT
            lpep_pickup_datetime,
            bronze_source_month AS source_month
        FROM base
    )

    UNION ALL

    -- Cleaned fields should not be NULL
    SELECT
        'Missing cleaned fields',
        'NULL',
        COUNT(*),
        COUNT_IF(
            RatecodeID IS NULL
            OR payment_type IS NULL
            OR trip_type IS NULL
            OR congestion_surcharge IS NULL
        ),
        ROUND(
            COUNT_IF(
                RatecodeID IS NULL
                OR payment_type IS NULL
                OR trip_type IS NULL
                OR congestion_surcharge IS NULL
            ) * 100.0 / COUNT(*),
            2
        ),
        'Cleaned fields should not be NULL'
    FROM base

    UNION ALL

    -- Required trip fields
    SELECT
        'Missing required trip fields',
        'NULL',
        COUNT(*),
        COUNT_IF(
            lpep_pickup_datetime IS NULL
            OR lpep_dropoff_datetime IS NULL
            OR PULocationID IS NULL
            OR DOLocationID IS NULL
        ),
        ROUND(
            COUNT_IF(
                lpep_pickup_datetime IS NULL
                OR lpep_dropoff_datetime IS NULL
                OR PULocationID IS NULL
                OR DOLocationID IS NULL
            ) * 100.0 / COUNT(*),
            2
        ),
        'Pickup, dropoff, PU location and DO location populated'
    FROM base

    UNION ALL

    -- Store-and-forward flag standardization
    SELECT
        'Unstandardized store_and_fwd_flag',
        'STANDARDIZATION',
        COUNT(*),
        COUNT_IF(
            store_and_fwd_flag IS NULL
            OR store_and_fwd_flag != TRIM(store_and_fwd_flag)
            OR store_and_fwd_flag NOT IN ('Y', 'N', 'Unknown')
        ),
        ROUND(
            COUNT_IF(
                store_and_fwd_flag IS NULL
                OR store_and_fwd_flag != TRIM(store_and_fwd_flag)
                OR store_and_fwd_flag NOT IN ('Y', 'N', 'Unknown')
            ) * 100.0 / COUNT(*),
            2
        ),
        'Y, N, or Unknown'
    FROM base

    UNION ALL

    -- Natural key uniqueness
    SELECT
        'Duplicate trip natural key',
        'UNIQUE',
        COUNT(*),
        COUNT(*) - COUNT(
            DISTINCT STRUCT(
                VendorID,
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                trip_distance,
                total_amount
            )
        ),
        ROUND(
            (
                COUNT(*) - COUNT(
                    DISTINCT STRUCT(
                        VendorID,
                        lpep_pickup_datetime,
                        lpep_dropoff_datetime,
                        PULocationID,
                        DOLocationID,
                        trip_distance,
                        total_amount
                    )
                )
            ) * 100.0 / COUNT(*),
            2
        ),
        '0 duplicate natural keys'
    FROM base

    UNION ALL

    -- Passenger count: NULL / zero = WARN, negative = FAIL
    SELECT
        'Invalid passenger count',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            passenger_count IS NULL
            OR passenger_count <= 0
        ),
        ROUND(
            COUNT_IF(
                passenger_count IS NULL
                OR passenger_count <= 0
            ) * 100.0 / COUNT(*),
            2
        ),
        'Passenger count > 0; NULL/0 = WARN, negative = FAIL'
    FROM base

    UNION ALL

    -- Negative passenger count is a separate critical condition
    SELECT
        'Negative passenger count',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(passenger_count < 0),
        ROUND(
            COUNT_IF(passenger_count < 0) * 100.0 / COUNT(*),
            2
        ),
        'Passenger count must not be negative'
    FROM base

    UNION ALL

    -- Total amount should not be negative
    SELECT
        'Invalid total amount',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(total_amount < 0),
        ROUND(
            COUNT_IF(total_amount < 0) * 100.0 / COUNT(*),
            2
        ),
        'total_amount >= 0'
    FROM base

    UNION ALL

    -- Trip datetime validity
    SELECT
        'Invalid trip datetime range',
        'VALIDITY',
        COUNT(*),
        COUNT_IF(
            lpep_pickup_datetime IS NULL
            OR lpep_dropoff_datetime IS NULL
            OR lpep_dropoff_datetime < lpep_pickup_datetime
        ),
        ROUND(
            COUNT_IF(
                lpep_pickup_datetime IS NULL
                OR lpep_dropoff_datetime IS NULL
                OR lpep_dropoff_datetime < lpep_pickup_datetime
            ) * 100.0 / COUNT(*),
            2
        ),
        'Dropoff >= pickup'
    FROM base

    UNION ALL

    -- Trip distance validity
    SELECT
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
            ) * 100.0 / COUNT(*),
            2
        ),
        'trip_distance > 0'
    FROM base
),

evaluated AS (
    SELECT
        *,
        CASE
            -- Passenger count: negative is a FAIL
            WHEN check_name = 'Negative passenger count'
                AND failures > 0
                THEN 'FAIL'

            -- Passenger count: NULL / zero is only a WARN
            WHEN check_name = 'Invalid passenger count'
                AND failures > 0
                THEN 'WARN'

            -- Volume tolerance: <= 2% = PASS, <= 5% = WARN
            WHEN check_type = 'VOLUME' AND failure_pct = 0
                THEN 'PASS'
            WHEN check_type = 'VOLUME' AND failure_pct <= 2
                THEN 'PASS'
            WHEN check_type = 'VOLUME' AND failure_pct <= 5
                THEN 'WARN'

            -- Uniqueness: <= 1% = WARN, otherwise FAIL
            WHEN check_type = 'UNIQUE' AND failure_pct = 0
                THEN 'PASS'
            WHEN check_type = 'UNIQUE' AND failure_pct <= 1
                THEN 'WARN'

            -- NULL checks: <= 1% = WARN
            WHEN check_type = 'NULL' AND failure_pct = 0
                THEN 'PASS'
            WHEN check_type = 'NULL' AND failure_pct <= 1
                THEN 'WARN'

            -- Standardization / validity / lineage
            WHEN check_type IN (
                'STANDARDIZATION',
                'VALIDITY',
                'LINEAGE'
            )
            AND failure_pct = 0
                THEN 'PASS'

            WHEN check_type IN (
                'STANDARDIZATION',
                'VALIDITY',
                'LINEAGE'
            )
            AND failure_pct <= 1
                THEN 'WARN'

            ELSE 'FAIL'
        END AS status
    FROM dq_results
)

SELECT
    '{dq_run_id}' AS dq_run_id,
    current_timestamp() AS dq_run_timestamp,
    check_name,
    check_type,
    records_checked,
    failures,
    failure_pct,
    expected_value,
    status
FROM evaluated
ORDER BY
    CASE status
        WHEN 'FAIL' THEN 1
        WHEN 'WARN' THEN 2
        WHEN 'PASS' THEN 3
    END,
    check_name
""")

# Save DQ results for audit/history
dq_result.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("nyc.nyc_quality.dq_results")

display(dq_result)

# Block downstream pipeline if any critical DQ check fails
failed_count = dq_result.filter("status = 'FAIL'").count()
warn_count = dq_result.filter("status = 'WARN'").count()

if failed_count > 0:
    raise Exception(
        f"DQ FAILED: {failed_count} check(s) failed. "
        f"Pipeline stopped. Review nyc.nyc_quality.dq_results."
    )

print(
    f"DQ PASSED: {warn_count} warning(s), "
    f"0 failures. Pipeline may continue."
)