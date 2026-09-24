-- DQ WARN Monitoring
--
-- Purpose:
-- Capture DQ WARN results for monitoring without blocking the pipeline.
-- WARN records remain in Silver and are tracked for trend analysis.

-- Create centralized WARN monitoring table
CREATE TABLE IF NOT EXISTS nyc.nyc_quality.dq_warn_monitoring (
    dq_run_id STRING,
    dq_run_timestamp TIMESTAMP,
    table_name STRING,
    category STRING,
    check_name STRING,
    check_type STRING,
    records_checked BIGINT,
    warnings BIGINT,
    warning_pct DOUBLE,
    expected_value STRING,
    monitored_at TIMESTAMP
);


-- Capture new WARN results from dq_results
INSERT INTO nyc.nyc_quality.dq_warn_monitoring (
    dq_run_id,
    dq_run_timestamp,
    table_name,
    category,
    check_name,
    check_type,
    records_checked,
    warnings,
    warning_pct,
    expected_value,
    monitored_at
)
SELECT
    dq_run_id,
    dq_run_timestamp,
    table_name,
    category,
    check_name,
    check_type,
    records_checked,
    failures AS warnings,
    failure_pct AS warning_pct,
    expected_value,
    current_timestamp() AS monitored_at

FROM nyc.nyc_quality.dq_results

WHERE status = 'WARN'

  -- Prevent duplicate monitoring records
  AND NOT EXISTS (
      SELECT 1
      FROM nyc.nyc_quality.dq_warn_monitoring m
      WHERE m.dq_run_id = dq_results.dq_run_id
        AND m.table_name = dq_results.table_name
        AND m.check_name = dq_results.check_name
  );


-- Compare each WARN result with the previous DQ run
WITH ranked_warns AS (
    SELECT
        table_name,
        category,
        check_name,
        dq_run_id,
        dq_run_timestamp,
        warnings,
        warning_pct,

        LAG(warning_pct) OVER (
            PARTITION BY table_name, check_name
            ORDER BY dq_run_timestamp
        ) AS previous_warning_pct

    FROM nyc.nyc_quality.dq_warn_monitoring
)

SELECT
    table_name,
    category,
    check_name,
    dq_run_id,
    dq_run_timestamp,
    warnings,
    warning_pct,
    previous_warning_pct,

    ROUND(
        warning_pct - previous_warning_pct,
        2
    ) AS change_in_pct_points,

    CASE
        WHEN previous_warning_pct IS NULL
            THEN 'FIRST RUN'

        WHEN warning_pct > previous_warning_pct
            THEN 'INCREASING'

        WHEN warning_pct < previous_warning_pct
            THEN 'DECREASING'

        ELSE 'STABLE'
    END AS trend

FROM ranked_warns

ORDER BY
    dq_run_timestamp DESC,
    check_name;