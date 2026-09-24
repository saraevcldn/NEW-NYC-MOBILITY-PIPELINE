-- CREATE GREEN TAXI QUARANTINE TABLE
-- Purpose:
-- Identify Green Taxi Silver records that violate row-level DQ FAIL rules
-- and store those records in the quarantine table for investigation/recovery.

-- Flow:
-- 1. Get the latest DQ run
-- 2. Identify which checks failed
-- 3. Calculate duplicate counts
-- 4. Identify failed records
-- 5. Insert failed records into quarantine

-- Dataset-level checks such as volume are not quarantined because they
-- do not identify a specific bad record.

CREATE TABLE IF NOT EXISTS nyc.nyc_quality.green_taxi_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY,

    -- DQ and quarantine metadata
    dq_run_id STRING,
    table_name STRING,
    check_name STRING,
    failure_reason STRING,
    dq_errors ARRAY<STRING>,
    quarantined_at TIMESTAMP,
    is_resolved BOOLEAN,

    -- Green Taxi Silver record
    trip_key BIGINT,
    VendorID INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE,
    cbd_congestion_fee DOUBLE,

    -- Source and ingestion lineage
    bronze_source_file STRING,
    bronze_source_month STRING,
    bronze_ingestion_timestamp TIMESTAMP,
    bronze_ingestion_date DATE,
    silver_ingestion_timestamp TIMESTAMP,
    silver_ingestion_date DATE
);

-- Get the latest DQ run

WITH latest_dq_run AS (
    SELECT dq_run_id
    FROM nyc.nyc_quality.dq_results
    ORDER BY dq_run_timestamp DESC
    LIMIT 1
),

-- Get only the checks that FAILED in the latest run

failed_checks AS (
    SELECT
        check_name
    FROM nyc.nyc_quality.dq_results
    WHERE dq_run_id = (SELECT dq_run_id FROM latest_dq_run)
      AND status = 'FAIL'
),

-- Convert failed checks into flags
-- This avoids using subqueries inside higher-order functions

fail_flags AS (
    SELECT
        MAX(CASE
            WHEN check_name = 'Missing pickup datetime'
            THEN 1 ELSE 0
        END) AS missing_pickup_datetime,

        MAX(CASE
            WHEN check_name = 'Missing dropoff datetime'
            THEN 1 ELSE 0
        END) AS missing_dropoff_datetime,

        MAX(CASE
            WHEN check_name = 'Missing pickup location'
            THEN 1 ELSE 0
        END) AS missing_pickup_location,

        MAX(CASE
            WHEN check_name = 'Missing dropoff location'
            THEN 1 ELSE 0
        END) AS missing_dropoff_location,

        MAX(CASE
            WHEN check_name = 'Negative passenger count'
            THEN 1 ELSE 0
        END) AS negative_passenger_count,

        MAX(CASE
            WHEN check_name = 'Invalid trip distance'
            THEN 1 ELSE 0
        END) AS invalid_trip_distance,

        MAX(CASE
            WHEN check_name = 'Invalid trip datetime range'
            THEN 1 ELSE 0
        END) AS invalid_trip_datetime_range,

        MAX(CASE
            WHEN check_name = 'Invalid total amount'
            THEN 1 ELSE 0
        END) AS invalid_total_amount,

        MAX(CASE
            WHEN check_name = 'Invalid fare amount'
            THEN 1 ELSE 0
        END) AS invalid_fare_amount,

        MAX(CASE
            WHEN check_name = 'Duplicate or missing trip_key'
            THEN 1 ELSE 0
        END) AS duplicate_trip_key,

        MAX(CASE
            WHEN check_name = 'Duplicate natural key'
            THEN 1 ELSE 0
        END) AS duplicate_natural_key

    FROM failed_checks
),

-- Calculate duplicate counts needed for row-level quarantine

silver_with_duplicate_counts AS (
    SELECT
        s.*,

        COUNT(*) OVER (
            PARTITION BY trip_key
        ) AS trip_key_count,

        COUNT(*) OVER (
            PARTITION BY
                VendorID,
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                trip_distance,
                total_amount
        ) AS natural_key_count

    FROM nyc.nyc_silver.green_taxi_silver s
),

-- Identify records that violated row-level FAIL rules

failed_records AS (
    SELECT
        s.*,

        filter(
            array(

                CASE
                    WHEN s.lpep_pickup_datetime IS NULL
                         AND f.missing_pickup_datetime = 1
                    THEN 'Missing pickup datetime'
                END,

                CASE
                    WHEN s.lpep_dropoff_datetime IS NULL
                         AND f.missing_dropoff_datetime = 1
                    THEN 'Missing dropoff datetime'
                END,

                CASE
                    WHEN s.PULocationID IS NULL
                         AND f.missing_pickup_location = 1
                    THEN 'Missing pickup location'
                END,

                CASE
                    WHEN s.DOLocationID IS NULL
                         AND f.missing_dropoff_location = 1
                    THEN 'Missing dropoff location'
                END,

                CASE
                    WHEN s.passenger_count < 0
                         AND f.negative_passenger_count = 1
                    THEN 'Negative passenger count'
                END,

                CASE
                    WHEN (s.trip_distance IS NULL OR s.trip_distance <= 0)
                         AND f.invalid_trip_distance = 1
                    THEN 'Invalid trip distance'
                END,

                CASE
                    WHEN s.lpep_dropoff_datetime < s.lpep_pickup_datetime
                         AND f.invalid_trip_datetime_range = 1
                    THEN 'Invalid trip datetime range'
                END,

                CASE
                    WHEN (s.total_amount IS NULL OR s.total_amount < 0)
                         AND f.invalid_total_amount = 1
                    THEN 'Invalid total amount'
                END,

                CASE
                    WHEN (s.fare_amount IS NULL OR s.fare_amount < 0)
                         AND f.invalid_fare_amount = 1
                    THEN 'Invalid fare amount'
                END,

                CASE
                    WHEN (s.trip_key IS NULL OR s.trip_key_count > 1)
                         AND f.duplicate_trip_key = 1
                    THEN 'Duplicate or missing trip_key'
                END,

                CASE
                    WHEN s.natural_key_count > 1
                         AND f.duplicate_natural_key = 1
                    THEN 'Duplicate natural key'
                END

            ),
            x -> x IS NOT NULL
        ) AS dq_errors

    FROM silver_with_duplicate_counts s

    -- Apply the latest DQ run's failed-check flags to every Silver record

    CROSS JOIN fail_flags f
)

-- Insert failed records into quarantine
-- quarantine_id is generated automatically by the identity column

INSERT INTO nyc.nyc_quality.green_taxi_quarantine (
    dq_run_id,
    table_name,
    check_name,
    failure_reason,
    dq_errors,
    quarantined_at,
    is_resolved,

    trip_key,
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    cbd_congestion_fee,

    bronze_source_file,
    bronze_source_month,
    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date
)

SELECT
    (SELECT dq_run_id FROM latest_dq_run) AS dq_run_id,

    'green_taxi_silver' AS table_name,

    -- First error is used as the primary check name/reason
    element_at(dq_errors, 1) AS check_name,
    element_at(dq_errors, 1) AS failure_reason,

    -- Keep all failed rules for the record
    dq_errors,

    current_timestamp() AS quarantined_at,

    -- New quarantine records are unresolved by default
    FALSE AS is_resolved,

    -- Original Green Taxi Silver record
    trip_key,
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    cbd_congestion_fee,

    -- Lineage information
    bronze_source_file,
    bronze_source_month,
    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date

FROM failed_records

-- Only insert records with at least one FAIL reason

WHERE size(dq_errors) > 0;