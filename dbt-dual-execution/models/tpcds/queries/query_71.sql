select i_brand_id brand_id, i_brand brand, t_hour, t_minute, sum(ext_price) ext_price
from
    {{ ref("item") }},
    (
        select
            ws_ext_sales_price as ext_price,
            ws_sold_date_sk as sold_date_sk,
            ws_item_sk as sold_item_sk,
            ws_sold_time_sk as time_sk
        from {{ ref("web_sales") }}, {{ ref("date_dim") }}
        where d_date_sk = ws_sold_date_sk and d_moy = 11 and d_year = 1999
        union all
        select
            cs_ext_sales_price as ext_price,
            cs_sold_date_sk as sold_date_sk,
            cs_item_sk as sold_item_sk,
            cs_sold_time_sk as time_sk
        from {{ ref("catalog_sales") }}, {{ ref("date_dim") }}
        where d_date_sk = cs_sold_date_sk and d_moy = 11 and d_year = 1999
        union all
        select
            ss_ext_sales_price as ext_price,
            ss_sold_date_sk as sold_date_sk,
            ss_item_sk as sold_item_sk,
            ss_sold_time_sk as time_sk
        from {{ ref("store_sales") }}, {{ ref("date_dim") }}
        where d_date_sk = ss_sold_date_sk and d_moy = 11 and d_year = 1999
    ) tmp,
    {{ ref("time_dim") }}
where
    sold_item_sk = i_item_sk
    and i_manager_id = 1
    and time_sk = t_time_sk
    and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
group by i_brand, i_brand_id, t_hour, t_minute
order by ext_price desc nulls first, i_brand_id nulls first, t_hour nulls first
