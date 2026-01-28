SELECT
    regexp_extract(url, 'http[s]?://([^/]+)/', 1) AS domain,
    count(*) AS count
FROM {{ source('hn_external', 'hacker_news_2024_2025') }}
WHERE url IS NOT NULL AND regexp_extract(url, 'http[s]?://([^/]+)/', 1) != ''
GROUP BY domain
ORDER BY count DESC
LIMIT 20 
