-- Taxi Zones quarantine table
-- Stores Taxi Zone records that fail row-level DQ checks.
-- Dataset-level failures such as volume remain in dq_results.

CREATE TABLE IF NOT EXISTS nyc.nyc_quality.taxi_zones_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY,

    dq_run_id STRING,
    table_name STRING,
    check_name STRING,
    failure_reason STRING,
    dq_errors ARRAY<STRING>,
    quarantined_at TIMESTAMP,
    is_resolved BOOLEAN,

    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING,

    bronze_ingestion_timestamp TIMESTAMP,
    bronze_ingestion_date DATE,
    silver_ingestion_timestamp TIMESTAMP,
    silver_ingestion_date DATE
);

WITH latest_dq_run AS (
    SELECT
        dq_run_id
    FROM nyc.nyc_quality.dq_results
    WHERE table_name = 'taxi_zones_silver'
    ORDER BY dq_run_timestamp DESC
    LIMIT 1
),

failed_checks AS (
    SELECT
        check_name
    FROM nyc.nyc_quality.dq_results
    WHERE table_name = 'taxi_zones_silver'
      AND dq_run_id = (SELECT dq_run_id FROM latest_dq_run)
      AND status = 'FAIL'
),

fail_flags AS (
    SELECT
        MAX(CASE
            WHEN check_name = 'Missing location_id'
            THEN 1 ELSE 0
        END) AS missing_location_id,

        MAX(CASE
            WHEN check_name = 'Missing borough'
            THEN 1 ELSE 0
        END) AS missing_borough,

        MAX(CASE
            WHEN check_name = 'Missing zone'
            THEN 1 ELSE 0
        END) AS missing_zone,

        MAX(CASE
            WHEN check_name = 'Missing service zone'
            THEN 1 ELSE 0
        END) AS missing_service_zone,

        MAX(CASE
            WHEN check_name = 'Missing bronze ingestion timestamp'
            THEN 1 ELSE 0
        END) AS missing_bronze_timestamp,

        MAX(CASE
            WHEN check_name = 'Missing bronze ingestion date'
            THEN 1 ELSE 0
        END) AS missing_bronze_date,

        MAX(CASE
            WHEN check_name = 'Missing silver ingestion timestamp'
            THEN 1 ELSE 0
        END) AS missing_silver_timestamp,

        MAX(CASE
            WHEN check_name = 'Missing silver ingestion date'
            THEN 1 ELSE 0
        END) AS missing_silver_date,

        MAX(CASE
            WHEN check_name = 'Duplicate location_id'
            THEN 1 ELSE 0
        END) AS duplicate_location_id,

        MAX(CASE
            WHEN check_name = 'Invalid location_id'
            THEN 1 ELSE 0
        END) AS invalid_location_id,

        MAX(CASE
            WHEN check_name = 'Invalid borough value'
            THEN 1 ELSE 0
        END) AS invalid_borough,

        MAX(CASE
            WHEN check_name = 'Invalid service_zone value'
            THEN 1 ELSE 0
        END) AS invalid_service_zone,

        MAX(CASE
            WHEN check_name = 'Unstandardized text values'
            THEN 1 ELSE 0
        END) AS unstandardized_text

    FROM failed_checks
),

taxi_zones_with_duplicates AS (
    SELECT
        z.*,

        COUNT(*) OVER (
            PARTITION BY location_id
        ) AS location_id_count

    FROM nyc.nyc_silver.taxi_zones_silver z
),

failed_records AS (
    SELECT
        z.*,

        filter(
            array(

                CASE
                    WHEN f.missing_location_id = 1
                         AND z.location_id IS NULL
                    THEN 'Missing location_id'
                END,

                CASE
                    WHEN f.missing_borough = 1
                         AND z.borough IS NULL
                    THEN 'Missing borough'
                END,

                CASE
                    WHEN f.missing_zone = 1
                         AND z.zone IS NULL
                    THEN 'Missing zone'
                END,

                CASE
                    WHEN f.missing_service_zone = 1
                         AND z.service_zone IS NULL
                    THEN 'Missing service zone'
                END,

                CASE
                    WHEN f.missing_bronze_timestamp = 1
                         AND z.bronze_ingestion_timestamp IS NULL
                    THEN 'Missing bronze ingestion timestamp'
                END,

                CASE
                    WHEN f.missing_bronze_date = 1
                         AND z.bronze_ingestion_date IS NULL
                    THEN 'Missing bronze ingestion date'
                END,

                CASE
                    WHEN f.missing_silver_timestamp = 1
                         AND z.silver_ingestion_timestamp IS NULL
                    THEN 'Missing silver ingestion timestamp'
                END,

                CASE
                    WHEN f.missing_silver_date = 1
                         AND z.silver_ingestion_date IS NULL
                    THEN 'Missing silver ingestion date'
                END,

                CASE
                    WHEN f.duplicate_location_id = 1
                         AND z.location_id IS NOT NULL
                         AND z.location_id_count > 1
                    THEN 'Duplicate location_id'
                END,

                CASE
                    WHEN f.invalid_location_id = 1
                         AND (
                             z.location_id IS NULL
                             OR z.location_id <= 0
                         )
                    THEN 'Invalid location_id'
                END,

                CASE
                    WHEN f.invalid_borough = 1
                         AND z.borough IS NOT NULL
                         AND UPPER(TRIM(z.borough)) NOT IN (
                             'EWR',
                             'QUEENS',
                             'BRONX',
                             'MANHATTAN',
                             'STATEN ISLAND',
                             'BROOKLYN',
                             'UNKNOWN',
                             'N/A'
                         )
                    THEN 'Invalid borough value'
                END,

                CASE
                    WHEN f.invalid_service_zone = 1
                         AND z.service_zone IS NOT NULL
                         AND UPPER(TRIM(z.service_zone)) NOT IN (
                             'EWR',
                             'BORO ZONE',
                             'YELLOW ZONE',
                             'AIRPORTS',
                             'N/A'
                         )
                    THEN 'Invalid service_zone value'
                END,

                CASE
                    WHEN f.unstandardized_text = 1
                         AND z.borough IS NOT NULL
                         AND z.borough != UPPER(TRIM(z.borough))
                    THEN 'Unstandardized borough'
                END,

                CASE
                    WHEN f.unstandardized_text = 1
                         AND z.zone IS NOT NULL
                         AND z.zone != TRIM(z.zone)
                    THEN 'Unstandardized zone'
                END,

                CASE
                    WHEN f.unstandardized_text = 1
                         AND z.service_zone IS NOT NULL
                         AND z.service_zone != UPPER(TRIM(z.service_zone))
                    THEN 'Unstandardized service_zone'
                END

            ),
            x -> x IS NOT NULL
        ) AS dq_errors

    FROM taxi_zones_with_duplicates z
    CROSS JOIN fail_flags f
)

INSERT INTO nyc.nyc_quality.taxi_zones_quarantine (
    dq_run_id,
    table_name,
    check_name,
    failure_reason,
    dq_errors,
    quarantined_at,
    is_resolved,

    location_id,
    borough,
    zone,
    service_zone,

    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date
)

SELECT
    (SELECT dq_run_id FROM latest_dq_run) AS dq_run_id,
    'taxi_zones_silver' AS table_name,
    element_at(dq_errors, 1) AS check_name,
    element_at(dq_errors, 1) AS failure_reason,
    dq_errors,
    current_timestamp() AS quarantined_at,
    false AS is_resolved,

    location_id,
    borough,
    zone,
    service_zone,

    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date

FROM failed_records
WHERE size(dq_errors) > 0;