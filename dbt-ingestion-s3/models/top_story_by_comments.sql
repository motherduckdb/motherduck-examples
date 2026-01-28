WITH ranked_stories AS (
    SELECT
        title,
        'https://news.ycombinator.com/item?id=' || id AS hn_url,
        descendants AS nb_comments,
        YEAR(timestamp) AS year,
        MONTH(timestamp) AS month,
        ROW_NUMBER() OVER (
            PARTITION BY YEAR(timestamp), MONTH(timestamp)
            ORDER BY descendants DESC
        ) AS rn
    FROM {{ source('hn_external', 'hacker_news_2024_2025') }}
    WHERE type = 'story'
)
SELECT
    year,
    month,
    title,
    hn_url,
    nb_comments
FROM ranked_stories
WHERE rn = 1
ORDER BY year, month 
