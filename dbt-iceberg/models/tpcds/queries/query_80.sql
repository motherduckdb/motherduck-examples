with
    ssr as (
        select
            s_store_id as store_id,
            sum(ss_ext_sales_price) as sales,
            sum(coalesce(sr_return_amt, 0)) as returns_,
            sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
        from {{ ref("store_sales") }}
        left outer join
            {{ ref("store_returns") }}
            on (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
            {{ ref("date_dim") }},
            {{ ref("store") }},
            {{ ref("item") }},
            {{ ref("promotion") }}
        where
            ss_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and ss_store_sk = s_store_sk
            and ss_item_sk = i_item_sk
            and i_current_price > 50
            and ss_promo_sk = p_promo_sk
            and p_channel_tv = 'N'
        group by s_store_id
    ),
    csr as (
        select
            cp_catalog_page_id as catalog_page_id,
            sum(cs_ext_sales_price) as sales,
            sum(coalesce(cr_return_amount, 0)) as returns_,
            sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
        from {{ ref("catalog_sales") }}
        left outer join
            {{ ref("catalog_returns") }}
            on (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
            {{ ref("date_dim") }},
            {{ ref("catalog_page") }},
            {{ ref("item") }},
            {{ ref("promotion") }}
        where
            cs_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and cs_catalog_page_sk = cp_catalog_page_sk
            and cs_item_sk = i_item_sk
            and i_current_price > 50
            and cs_promo_sk = p_promo_sk
            and p_channel_tv = 'N'
        group by cp_catalog_page_id
    ),
    wsr as (
        select
            web_site_id,
            sum(ws_ext_sales_price) as sales,
            sum(coalesce(wr_return_amt, 0)) as returns_,
            sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
        from {{ ref("web_sales") }}
        left outer join
            {{ ref("web_returns") }}
            on (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
            {{ ref("date_dim") }},
            {{ ref("web_site") }},
            {{ ref("item") }},
            {{ ref("promotion") }}
        where
            ws_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and ws_web_site_sk = web_site_sk
            and ws_item_sk = i_item_sk
            and i_current_price > 50
            and ws_promo_sk = p_promo_sk
            and p_channel_tv = 'N'
        group by web_site_id
    )
select
    channel, id, sum(sales) as sales, sum(returns_) as returns_, sum(profit) as profit
from (
    SELECT 'store channel' AS channel ,
          concat('store', store_id) AS id ,
          sales ,
          returns_ ,
          profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel ,
                    concat('catalog_page', catalog_page_id) AS id ,
                    sales ,
                    returns_ ,
                    profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel ,
                    concat('web_site', web_site_id) AS id ,
                    sales ,
                    returns_ ,
                    profit
   FROM wsr ) x
group by rollup (channel, id)
order by channel nulls first, id nulls first
limit 100
