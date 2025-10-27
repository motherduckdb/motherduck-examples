{{
  config(
    materialized='table'
  )
}}

WITH date_spine AS (
    SELECT
        CAST(date_day AS DATE) AS date_day
    FROM (
        SELECT
            UNNEST(generate_series(
                DATE '2024-01-01',
                DATE '2025-12-31',
                INTERVAL '1 day'
            )) AS date_day
    ) dates
)

SELECT
    date_day
FROM date_spine
