-- GREEN TAXI - SILVER TABLE
-- Clean, deduplicate, and MERGE Bronze -> Silver

-- Create the Silver table only if it does not exist yet.
CREATE TABLE IF NOT EXISTS nyc.nyc_silver.green_taxi_silver (

    -- Surrogate key generated
    trip_key BIGINT GENERATED ALWAYS AS IDENTITY,

    -- Green Taxi trip data
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

    -- Bronze lineage
    bronze_source_file STRING,
    bronze_source_month STRING,
    bronze_ingestion_timestamp TIMESTAMP,
    bronze_ingestion_date DATE,

    -- Silver processing metadata
    silver_ingestion_timestamp TIMESTAMP,
    silver_ingestion_date DATE
);

-- CLEAN, DEDUPLICATE, AND MERGE BRONZE -> SILVER
MERGE INTO nyc.nyc_silver.green_taxi_silver AS target

USING (
    SELECT
        VendorID,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,

        -- Clean string field
        TRIM(
            COALESCE(store_and_fwd_flag, 'Unknown')
        ) AS store_and_fwd_flag,

        -- Replace missing values
        COALESCE(RatecodeID, -1) AS RatecodeID,
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

        COALESCE(payment_type, -1) AS payment_type,
        COALESCE(trip_type, -1) AS trip_type,
        COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge,
        cbd_congestion_fee,

        -- Bronze lineage
        source_file AS bronze_source_file,
        source_month AS bronze_source_month,
        bronze_ingestion_timestamp,
        bronze_ingestion_date,

        -- Silver processing metadata
        current_timestamp() AS silver_ingestion_timestamp,
        current_date() AS silver_ingestion_date

    FROM (

        SELECT
            *,

            -- Deduplicate using the composite natural key.
            -- Keep the most recently ingested record.
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
            -- Remove records missing essential trip information
            lpep_pickup_datetime IS NOT NULL
            AND lpep_dropoff_datetime IS NOT NULL
            AND PULocationID IS NOT NULL
            AND DOLocationID IS NOT NULL

            -- Drop invalid trip durations
            AND lpep_dropoff_datetime >= lpep_pickup_datetime

            -- Drop invalid financial values
            AND fare_amount >= 0
            AND total_amount >= 0

            -- Drop trips with no distance
            AND trip_distance > 0

            -- Project scope: March through May 2026 only
            AND lpep_pickup_datetime >= '2026-03-01'
            AND lpep_pickup_datetime < '2026-06-01'

            -- Make sure pickup month matches the source month
            AND date_format(
                lpep_pickup_datetime,
                'yyyy-MM'
            ) = source_month
    )

    -- Keep only one record per composite natural key
    WHERE row_num = 1

) AS source


-- COMPOSITE NATURAL KEY

-- Green Taxi does not provide a single unique trip ID.
-- These columns together identify the trip for MERGE purposes.
-- <=> is null-safe equality.

ON  target.VendorID <=> source.VendorID
AND target.lpep_pickup_datetime <=> source.lpep_pickup_datetime
AND target.lpep_dropoff_datetime <=> source.lpep_dropoff_datetime
AND target.PULocationID <=> source.PULocationID
AND target.DOLocationID <=> source.DOLocationID
AND target.trip_distance <=> source.trip_distance
AND target.total_amount <=> source.total_amount

-- UPDATE EXISTING RECORD

WHEN MATCHED THEN UPDATE SET

    target.store_and_fwd_flag = source.store_and_fwd_flag,
    target.RatecodeID = source.RatecodeID,
    target.passenger_count = source.passenger_count,
    target.fare_amount = source.fare_amount,
    target.extra = source.extra,
    target.mta_tax = source.mta_tax,
    target.tip_amount = source.tip_amount,
    target.tolls_amount = source.tolls_amount,
    target.ehail_fee = source.ehail_fee,
    target.improvement_surcharge = source.improvement_surcharge,
    target.payment_type = source.payment_type,
    target.trip_type = source.trip_type,
    target.congestion_surcharge = source.congestion_surcharge,
    target.cbd_congestion_fee = source.cbd_congestion_fee,

    -- Bronze lineage
    target.bronze_source_file = source.bronze_source_file,
    target.bronze_source_month = source.bronze_source_month,
    target.bronze_ingestion_timestamp = source.bronze_ingestion_timestamp,
    target.bronze_ingestion_date = source.bronze_ingestion_date,

    -- Silver processing metadata
    target.silver_ingestion_timestamp = source.silver_ingestion_timestamp,
    target.silver_ingestion_date = source.silver_ingestion_date



-- INSERT NEW RECORD

WHEN NOT MATCHED THEN INSERT (

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

    -- Bronze lineage
    bronze_source_file,
    bronze_source_month,
    bronze_ingestion_timestamp,
    bronze_ingestion_date,

    -- Silver processing metadata
    silver_ingestion_timestamp,
    silver_ingestion_date
)

VALUES (

    source.VendorID,
    source.lpep_pickup_datetime,
    source.lpep_dropoff_datetime,
    source.store_and_fwd_flag,
    source.RatecodeID,
    source.PULocationID,
    source.DOLocationID,
    source.passenger_count,
    source.trip_distance,
    source.fare_amount,
    source.extra,
    source.mta_tax,
    source.tip_amount,
    source.tolls_amount,
    source.ehail_fee,
    source.improvement_surcharge,
    source.total_amount,
    source.payment_type,
    source.trip_type,
    source.congestion_surcharge,
    source.cbd_congestion_fee,

    source.bronze_source_file,
    source.bronze_source_month,
    source.bronze_ingestion_timestamp,
    source.bronze_ingestion_date,

    source.silver_ingestion_timestamp,
    source.silver_ingestion_date
);
