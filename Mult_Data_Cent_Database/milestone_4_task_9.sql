WITH TimeDifferences AS (
    SELECT
        ddt.year,
        ddt.month,
        ddt.day,
        ddt.timestamp,
        LEAD(TO_TIMESTAMP(ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp, 'YYYY-MM-DD HH24:MI:SS')) OVER (PARTITION BY ddt.year ORDER BY ddt.year, ddt.month, ddt.day, ddt.timestamp) - TO_TIMESTAMP(ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS time_difference
    FROM
        public.dim_date_times ddt
),
AveragedDifferences AS (
    SELECT
        year,
        AVG(time_difference) AS avg_time_diff
    FROM
        TimeDifferences
    GROUP BY
        year
)
SELECT
    year,
    EXTRACT(hour FROM avg_time_diff) AS hours,
    EXTRACT(minute FROM avg_time_diff) AS minutes,
    EXTRACT(second FROM avg_time_diff) AS seconds
FROM
    AveragedDifferences
ORDER BY
    year;


