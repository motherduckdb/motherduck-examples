/*

    This dbt model aggregates sentiment metrics from product reviews by product (parent_asin).
    It calculates counts of positive, neutral, and negative sentiments for both overall reviews
    and customer service interactions. Additionally, it computes normalized sentiment scores 
    ranging from -1 to 1 for both categories:

    - sentiment_score: (positive - negative) / total reviews
    - service_sentiment_score: (positive - negative) / total service interactions

    The scores provide a quick way to assess customer satisfaction, with:
    * Positive scores indicating more positive than negative reviews
    * Negative scores indicating more negative than positive reviews
    * Scores closer to 0 indicating mixed or neutral sentiment

*/

{{ config(materialized="view") }}

SELECT 
    parent_asin,
    COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) as positive_count,
    COUNT(CASE WHEN sentiment = 'neutral' THEN 1 END) as neutral_count,
    COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) as negative_count,
    (positive_count - negative_count)::FLOAT / NULLIF(positive_count + neutral_count + negative_count, 0) as sentiment_score,
    COUNT(CASE WHEN customer_service_sentiment = 'positive' THEN 1 END) as positive_service_count,
    COUNT(CASE WHEN customer_service_sentiment = 'neutral' THEN 1 END) as neutral_service_count,
    COUNT(CASE WHEN customer_service_sentiment = 'negative' THEN 1 END) as negative_service_count,
    (positive_service_count - negative_service_count)::FLOAT / NULLIF(positive_service_count + neutral_service_count + negative_service_count, 0) as service_sentiment_score,
FROM {{ ref("reviews_attributes") }}
GROUP BY parent_asin
