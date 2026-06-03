with
    ss as (
        select
            s_store_sk, sum(ss_ext_sales_price) as sales, sum(ss_net_profit) as profit
        from {{ ref("store_sales") }}, {{ ref("date_dim") }}, {{ ref("store") }}
        where
            ss_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and ss_store_sk = s_store_sk
        group by s_store_sk
    ),
    sr as (
        select
            s_store_sk, sum(sr_return_amt) as returns_, sum(sr_net_loss) as profit_loss
        from {{ ref("store_returns") }}, {{ ref("date_dim") }}, {{ ref("store") }}
        where
            sr_returned_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and sr_store_sk = s_store_sk
        group by s_store_sk
    ),
    cs as (
        select
            cs_call_center_sk,
            sum(cs_ext_sales_price) as sales,
            sum(cs_net_profit) as profit
        from {{ ref("catalog_sales") }}, {{ ref("date_dim") }}
        where
            cs_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
        group by cs_call_center_sk
    ),
    cr as (
        select
            cr_call_center_sk,
            sum(cr_return_amount) as returns_,
            sum(cr_net_loss) as profit_loss
        from {{ ref("catalog_returns") }}, {{ ref("date_dim") }}
        where
            cr_returned_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
        group by cr_call_center_sk
    ),
    ws as (
        select
            wp_web_page_sk,
            sum(ws_ext_sales_price) as sales,
            sum(ws_net_profit) as profit
        from {{ ref("web_sales") }}, {{ ref("date_dim") }}, {{ ref("web_page") }}
        where
            ws_sold_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and ws_web_page_sk = wp_web_page_sk
        group by wp_web_page_sk
    ),
    wr as (
        select
            wp_web_page_sk,
            sum(wr_return_amt) as returns_,
            sum(wr_net_loss) as profit_loss
        from {{ ref("web_returns") }}, {{ ref("date_dim") }}, {{ ref("web_page") }}
        where
            wr_returned_date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-22' as date)
            and wr_web_page_sk = wp_web_page_sk
        group by wp_web_page_sk
    )
select
    channel, id, sum(sales) as sales, sum(returns_) as returns_, sum(profit) as profit
from
    (
        select
            'store channel' as channel,
            ss.s_store_sk as id,
            sales,
            coalesce(returns_, 0) as returns_,
            (profit - coalesce(profit_loss, 0)) as profit
        from ss
        left join sr on ss.s_store_sk = sr.s_store_sk
        union all
        select
            'catalog channel' as channel,
            cs_call_center_sk as id,
            sales,
            returns_,
            (profit - profit_loss) as profit
        from cs, cr
        union all
        select
            'web channel' as channel,
            ws.wp_web_page_sk as id,
            sales,
            coalesce(returns_, 0) returns_,
            (profit - coalesce(profit_loss, 0)) as profit
        from ws
        left join wr on ws.wp_web_page_sk = wr.wp_web_page_sk
    ) x
group by rollup (channel, id)
order by channel nulls first, id nulls first, returns_ desc
limit 100
