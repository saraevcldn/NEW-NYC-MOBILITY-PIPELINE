%python
"""
Purpose:
Detect increasing DQ WARN trends and fail the Databricks task when
a warning condition gets worse between DQ runs.

Flow:
dq_warn_monitoring → compare WARN trends → increasing? → fail task
"""

from pyspark.sql import SparkSession


# Start Spark session
spark = SparkSession.builder.getOrCreate()


# Compare the latest WARN result with the previous DQ run
query = """
WITH ranked_warns AS (
    SELECT
        table_name,
        category,
        check_name,
        dq_run_id,
        warning_pct,

        -- Get the previous warning percentage for comparison
        LAG(warning_pct) OVER (
            PARTITION BY table_name, check_name
            ORDER BY dq_run_timestamp
        ) AS previous_warning_pct,

         -- Rank WARN records from newest to oldest
        ROW_NUMBER() OVER (
            PARTITION BY table_name, check_name
            ORDER BY dq_run_timestamp DESC
        ) AS recency_rank

    FROM nyc.nyc_quality.dq_warn_monitoring
),

-- Keep only WARN checks that are getting worse
warning_alerts AS (
    SELECT
        table_name,
        category,
        check_name,
        dq_run_id,
        warning_pct,
        previous_warning_pct,

        -- Calculate the change in warning percentage points
        ROUND(warning_pct - previous_warning_pct, 2) AS change_in_pct_points

    FROM ranked_warns

    -- Only evaluate the latest WARN result
    WHERE recency_rank = 1
        AND previous_warning_pct IS NOT NULL
        AND warning_pct > previous_warning_pct
)

SELECT *
FROM warning_alerts
ORDER BY change_in_pct_points DESC
"""


# Run the alert query
alerts = spark.sql(query)

# Count increasing WARN conditions
alert_count = alerts.count()


# Fail the task when WARN conditions are increasing
if alert_count > 0:
    alerts.show(truncate=False)

    raise Exception(
        f"DQ warning alert triggered: "
        f"{alert_count} warning trend(s) are increasing."
    )


# No increasing WARN trends detected
print("No increasing DQ WARN trends detected.")