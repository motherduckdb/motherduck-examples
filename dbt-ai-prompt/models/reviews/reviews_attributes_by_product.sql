/*
    This dbt model processes review extracted attributes and aggregates them by product. 
    It takes the array fields from reviews_attributes and combines them into 
    distinct arrays per product, removing duplicates. This gives us a consolidated 
    view of all attributes, pros, cons, use cases etc. mentioned across all reviews 
    for each product.
*/
{{ config(materialized="view") }}


with
    unnested_array_attributes as (
        -- First unnest all arrays to get individual attributes
        select
            parent_asin,
            -- Feature mentions
            unnest(product_features) as product_features,
            unnest(pros) as pros,
            unnest(cons) as cons,
            -- Comparative analysis
            unnest(competitor_mentions) as competitor_mentions,
            -- Usage context
            unnest(use_case) as use_case,
            unnest(purchase_reason) as purchase_reason,
            unnest(reported_issues) as reported_issues,
            unnest(quality_concerns) as quality_concerns
        from {{ ref("reviews_attributes") }}
    )
select
    parent_asin,
    -- Feature mentions
    array_distinct(array_agg(product_features)) as product_features,
    array_distinct(array_agg(pros)) as pros,
    array_distinct(array_agg(cons)) as cons,
    -- Comparative analysis
    array_distinct(array_agg(competitor_mentions)) as competitor_mentions,
    -- Usage context
    array_distinct(array_agg(use_case)) as use_case,
    array_distinct(array_agg(purchase_reason)) as purchase_reason,
    array_distinct(array_agg(reported_issues)) as reported_issues,
    array_distinct(array_agg(quality_concerns)) as quality_concerns
from unnested_array_attributes
group by parent_asin
