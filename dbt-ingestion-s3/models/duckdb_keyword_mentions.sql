SELECT
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month,
    COUNT(*) AS keyword_mentions
FROM {{ source('hn_external', 'hacker_news_2024_2025') }}
WHERE
    (title LIKE '%duckdb%' OR text LIKE '%duckdb%')
GROUP BY year, month
ORDER BY year ASC, month ASC 
