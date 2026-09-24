-- Weather quarantine table
-- Stores Weather records that fail row-level DQ checks.
-- Dataset-level failures such as volume remain in dq_results.

CREATE TABLE IF NOT EXISTS nyc.nyc_quality.weather_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY,

    dq_run_id STRING,
    table_name STRING,
    check_name STRING,
    failure_reason STRING,
    dq_errors ARRAY<STRING>,
    quarantined_at TIMESTAMP,
    is_resolved BOOLEAN,

    timestamp TIMESTAMP,
    temperature_2m DOUBLE,
    precipitation DOUBLE,
    rain DOUBLE,
    snowfall DOUBLE,
    wind_speed_10m DOUBLE,
    weather_code BIGINT,

    source_file STRING,

    bronze_ingestion_timestamp TIMESTAMP,
    bronze_ingestion_date DATE,
    silver_ingestion_timestamp TIMESTAMP,
    silver_ingestion_date DATE
);

WITH latest_dq_run AS (
    SELECT
        dq_run_id
    FROM nyc.nyc_quality.dq_results
    WHERE table_name = 'clean_weather'
    ORDER BY dq_run_timestamp DESC
    LIMIT 1
),

failed_checks AS (
    SELECT
        check_name
    FROM nyc.nyc_quality.dq_results
    WHERE table_name = 'clean_weather'
      AND dq_run_id = (SELECT dq_run_id FROM latest_dq_run)
      AND status = 'FAIL'
),

fail_flags AS (
    SELECT
        MAX(CASE WHEN check_name = 'Missing timestamp'
                 THEN 1 ELSE 0 END) AS missing_timestamp,
        MAX(CASE WHEN check_name = 'Missing temperature'
                 THEN 1 ELSE 0 END) AS missing_temperature,
        MAX(CASE WHEN check_name = 'Missing precipitation'
                 THEN 1 ELSE 0 END) AS missing_precipitation,
        MAX(CASE WHEN check_name = 'Missing rain'
                 THEN 1 ELSE 0 END) AS missing_rain,
        MAX(CASE WHEN check_name = 'Missing snowfall'
                 THEN 1 ELSE 0 END) AS missing_snowfall,
        MAX(CASE WHEN check_name = 'Missing wind speed'
                 THEN 1 ELSE 0 END) AS missing_wind_speed,
        MAX(CASE WHEN check_name = 'Missing weather code'
                 THEN 1 ELSE 0 END) AS missing_weather_code,
        MAX(CASE WHEN check_name = 'Missing source file'
                 THEN 1 ELSE 0 END) AS missing_source_file,
        MAX(CASE WHEN check_name = 'Missing bronze ingestion timestamp'
                 THEN 1 ELSE 0 END) AS missing_bronze_timestamp,
        MAX(CASE WHEN check_name = 'Missing bronze ingestion date'
                 THEN 1 ELSE 0 END) AS missing_bronze_date,
        MAX(CASE WHEN check_name = 'Missing silver ingestion timestamp'
                 THEN 1 ELSE 0 END) AS missing_silver_timestamp,
        MAX(CASE WHEN check_name = 'Missing silver ingestion date'
                 THEN 1 ELSE 0 END) AS missing_silver_date,
        MAX(CASE WHEN check_name = 'Duplicate timestamp'
                 THEN 1 ELSE 0 END) AS duplicate_timestamp,
        MAX(CASE WHEN check_name = 'Invalid temperature'
                 THEN 1 ELSE 0 END) AS invalid_temperature,
        MAX(CASE WHEN check_name = 'Invalid precipitation'
                 THEN 1 ELSE 0 END) AS invalid_precipitation,
        MAX(CASE WHEN check_name = 'Invalid rain'
                 THEN 1 ELSE 0 END) AS invalid_rain,
        MAX(CASE WHEN check_name = 'Invalid snowfall'
                 THEN 1 ELSE 0 END) AS invalid_snowfall,
        MAX(CASE WHEN check_name = 'Invalid wind speed'
                 THEN 1 ELSE 0 END) AS invalid_wind_speed,
        MAX(CASE WHEN check_name = 'Invalid weather code'
                 THEN 1 ELSE 0 END) AS invalid_weather_code,
        MAX(CASE WHEN check_name = 'Precipitation less than rain'
                 THEN 1 ELSE 0 END) AS precipitation_less_than_rain,
        MAX(CASE WHEN check_name = 'Invalid timestamp alignment'
                 THEN 1 ELSE 0 END) AS invalid_timestamp_alignment
    FROM failed_checks
),

weather_with_duplicates AS (
    SELECT
        w.*,

        COUNT(*) OVER (
            PARTITION BY timestamp
        ) AS timestamp_count

    FROM nyc.nyc_silver.clean_weather w
),

failed_records AS (
    SELECT
        w.*,

        filter(
            array(
                CASE
                    WHEN f.missing_timestamp = 1
                         AND w.timestamp IS NULL
                    THEN 'Missing timestamp'
                END,

                CASE
                    WHEN f.missing_temperature = 1
                         AND w.temperature_2m IS NULL
                    THEN 'Missing temperature'
                END,

                CASE
                    WHEN f.missing_precipitation = 1
                         AND w.precipitation IS NULL
                    THEN 'Missing precipitation'
                END,

                CASE
                    WHEN f.missing_rain = 1
                         AND w.rain IS NULL
                    THEN 'Missing rain'
                END,

                CASE
                    WHEN f.missing_snowfall = 1
                         AND w.snowfall IS NULL
                    THEN 'Missing snowfall'
                END,

                CASE
                    WHEN f.missing_wind_speed = 1
                         AND w.wind_speed_10m IS NULL
                    THEN 'Missing wind speed'
                END,

                CASE
                    WHEN f.missing_weather_code = 1
                         AND w.weather_code IS NULL
                    THEN 'Missing weather code'
                END,

                CASE
                    WHEN f.missing_source_file = 1
                         AND w.source_file IS NULL
                    THEN 'Missing source file'
                END,

                CASE
                    WHEN f.missing_bronze_timestamp = 1
                         AND w.bronze_ingestion_timestamp IS NULL
                    THEN 'Missing bronze ingestion timestamp'
                END,

                CASE
                    WHEN f.missing_bronze_date = 1
                         AND w.bronze_ingestion_date IS NULL
                    THEN 'Missing bronze ingestion date'
                END,

                CASE
                    WHEN f.missing_silver_timestamp = 1
                         AND w.silver_ingestion_timestamp IS NULL
                    THEN 'Missing silver ingestion timestamp'
                END,

                CASE
                    WHEN f.missing_silver_date = 1
                         AND w.silver_ingestion_date IS NULL
                    THEN 'Missing silver ingestion date'
                END,

                CASE
                    WHEN f.duplicate_timestamp = 1
                         AND w.timestamp IS NOT NULL
                         AND w.timestamp_count > 1
                    THEN 'Duplicate timestamp'
                END,

                CASE
                    WHEN f.invalid_temperature = 1
                         AND (
                             w.temperature_2m IS NULL
                             OR w.temperature_2m < -50
                             OR w.temperature_2m > 50
                         )
                    THEN 'Invalid temperature'
                END,

                CASE
                    WHEN f.invalid_precipitation = 1
                         AND (
                             w.precipitation IS NULL
                             OR w.precipitation < 0
                             OR w.precipitation > 100
                         )
                    THEN 'Invalid precipitation'
                END,

                CASE
                    WHEN f.invalid_rain = 1
                         AND (
                             w.rain IS NULL
                             OR w.rain < 0
                             OR w.rain > 100
                         )
                    THEN 'Invalid rain'
                END,

                CASE
                    WHEN f.invalid_snowfall = 1
                         AND (
                             w.snowfall IS NULL
                             OR w.snowfall < 0
                             OR w.snowfall > 100
                         )
                    THEN 'Invalid snowfall'
                END,

                CASE
                    WHEN f.invalid_wind_speed = 1
                         AND (
                             w.wind_speed_10m IS NULL
                             OR w.wind_speed_10m < 0
                             OR w.wind_speed_10m > 100
                         )
                    THEN 'Invalid wind speed'
                END,

                CASE
                    WHEN f.invalid_weather_code = 1
                         AND w.weather_code NOT IN (
                             0, 1, 2, 3,
                             45, 48,
                             51, 53, 55,
                             56, 57,
                             61, 63, 65,
                             66, 67,
                             71, 73, 75,
                             77,
                             80, 81, 82,
                             85, 86,
                             95, 96, 99
                         )
                    THEN 'Invalid weather code'
                END,

                CASE
                    WHEN f.precipitation_less_than_rain = 1
                         AND w.precipitation < w.rain
                    THEN 'Precipitation less than rain'
                END,

                CASE
                    WHEN f.invalid_timestamp_alignment = 1
                         AND (
                             w.timestamp IS NULL
                             OR minute(w.timestamp) != 0
                             OR second(w.timestamp) != 0
                         )
                    THEN 'Invalid timestamp alignment'
                END
            ),
            x -> x IS NOT NULL
        ) AS dq_errors

    FROM weather_with_duplicates w
    CROSS JOIN fail_flags f
)

INSERT INTO nyc.nyc_quality.weather_quarantine (
    dq_run_id,
    table_name,
    check_name,
    failure_reason,
    dq_errors,
    quarantined_at,
    is_resolved,

    timestamp,
    temperature_2m,
    precipitation,
    rain,
    snowfall,
    wind_speed_10m,
    weather_code,

    source_file,

    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date
)

SELECT
    (SELECT dq_run_id FROM latest_dq_run) AS dq_run_id,
    'clean_weather' AS table_name,
    element_at(dq_errors, 1) AS check_name,
    element_at(dq_errors, 1) AS failure_reason,
    dq_errors,
    current_timestamp() AS quarantined_at,
    false AS is_resolved,

    timestamp,
    temperature_2m,
    precipitation,
    rain,
    snowfall,
    wind_speed_10m,
    weather_code,

    source_file,

    bronze_ingestion_timestamp,
    bronze_ingestion_date,
    silver_ingestion_timestamp,
    silver_ingestion_date

FROM failed_records
WHERE size(dq_errors) > 0;