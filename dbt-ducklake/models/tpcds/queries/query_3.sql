select
    dt.d_year,
    {{ ref("item") }}.i_brand_id brand_id,
    {{ ref("item") }}.i_brand brand,
    sum(ss_ext_sales_price) sum_agg
from {{ ref("date_dim") }} dt, {{ ref("store_sales") }}, {{ ref("item") }}
where
    dt.d_date_sk = {{ ref("store_sales") }}.ss_sold_date_sk
    and {{ ref("store_sales") }}.ss_item_sk = {{ ref("item") }}.i_item_sk
    and {{ ref("item") }}.i_manufact_id = 128
    and dt.d_moy = 11
group by dt.d_year, {{ ref("item") }}.i_brand, {{ ref("item") }}.i_brand_id
order by dt.d_year, sum_agg desc, brand_id
limit 100
