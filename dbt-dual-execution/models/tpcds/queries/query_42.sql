select
    dt.d_year,
    {{ ref("item") }}.i_category_id,
    {{ ref("item") }}.i_category,
    sum(ss_ext_sales_price)
from {{ ref("date_dim") }} dt, {{ ref("store_sales") }}, {{ ref("item") }}
where
    dt.d_date_sk = {{ ref("store_sales") }}.ss_sold_date_sk
    and {{ ref("store_sales") }}.ss_item_sk = {{ ref("item") }}.i_item_sk
    and {{ ref("item") }}.i_manager_id = 1
    and dt.d_moy = 11
    and dt.d_year = 2000
group by dt.d_year, {{ ref("item") }}.i_category_id, {{ ref("item") }}.i_category
order by
    sum(ss_ext_sales_price) desc,
    dt.d_year,
    {{ ref("item") }}.i_category_id,
    {{ ref("item") }}.i_category
limit 100
