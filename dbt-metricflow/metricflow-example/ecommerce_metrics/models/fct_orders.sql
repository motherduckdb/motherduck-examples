{{
  config(
    materialized='table'
  )
}}

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    status,
    amount
FROM {{ ref('raw_orders') }}
