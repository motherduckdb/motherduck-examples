select
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category,
    count(*) sales_cnt,
    sum(ext_sales_price) sales_amt
from
    (
        select
            'store' as channel,
            'ss_store_sk' col_name,
            d_year,
            d_qoy,
            i_category,
            ss_ext_sales_price ext_sales_price
        from {{ ref("store_sales") }}, {{ ref("item") }}, {{ ref("date_dim") }}
        where
            ss_store_sk is null
            and ss_sold_date_sk = d_date_sk
            and ss_item_sk = i_item_sk
        union all
        select
            'web' as channel,
            'ws_ship_customer_sk' col_name,
            d_year,
            d_qoy,
            i_category,
            ws_ext_sales_price ext_sales_price
        from {{ ref("web_sales") }}, {{ ref("item") }}, {{ ref("date_dim") }}
        where
            ws_ship_customer_sk is null
            and ws_sold_date_sk = d_date_sk
            and ws_item_sk = i_item_sk
        union all
        select
            'catalog' as channel,
            'cs_ship_addr_sk' col_name,
            d_year,
            d_qoy,
            i_category,
            cs_ext_sales_price ext_sales_price
        from {{ ref("catalog_sales") }}, {{ ref("item") }}, {{ ref("date_dim") }}
        where
            cs_ship_addr_sk is null
            and cs_sold_date_sk = d_date_sk
            and cs_item_sk = i_item_sk
    ) foo
group by channel, col_name, d_year, d_qoy, i_category
order by
    channel nulls first,
    col_name nulls first,
    d_year nulls first,
    d_qoy nulls first,
    i_category nulls first
limit 100
