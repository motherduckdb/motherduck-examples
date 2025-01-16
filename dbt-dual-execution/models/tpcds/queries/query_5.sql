with
    ssr as (
        select
            s_store_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns_,
            sum(net_loss) as profit_loss
        from
            (
                select
                    ss_store_sk as store_sk,
                    ss_sold_date_sk as date_sk,
                    ss_ext_sales_price as sales_price,
                    ss_net_profit as profit,
                    cast(0 as decimal(7, 2)) as return_amt,
                    cast(0 as decimal(7, 2)) as net_loss
                from {{ ref("store_sales") }}
                union all
                select
                    sr_store_sk as store_sk,
                    sr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    sr_return_amt as return_amt,
                    sr_net_loss as net_loss
                from {{ ref("store_returns") }}
            ) salesreturns,
            {{ ref("date_dim") }},
            {{ ref("store") }}
        where
            date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-06' as date)
            and store_sk = s_store_sk
        group by s_store_id
    ),
    csr as (
        select
            cp_catalog_page_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns_,
            sum(net_loss) as profit_loss
        from
            (
                select
                    cs_catalog_page_sk as page_sk,
                    cs_sold_date_sk as date_sk,
                    cs_ext_sales_price as sales_price,
                    cs_net_profit as profit,
                    cast(0 as decimal(7, 2)) as return_amt,
                    cast(0 as decimal(7, 2)) as net_loss
                from {{ ref("catalog_sales") }}
                union all
                select
                    cr_catalog_page_sk as page_sk,
                    cr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    cr_return_amount as return_amt,
                    cr_net_loss as net_loss
                from {{ ref("catalog_returns") }}
            ) salesreturns,
            {{ ref("date_dim") }},
            {{ ref("catalog_page") }}
        where
            date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-06' as date)
            and page_sk = cp_catalog_page_sk
        group by cp_catalog_page_id
    ),
    wsr as (
        select
            web_site_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns_,
            sum(net_loss) as profit_loss
        from
            (
                select
                    ws_web_site_sk as wsr_web_site_sk,
                    ws_sold_date_sk as date_sk,
                    ws_ext_sales_price as sales_price,
                    ws_net_profit as profit,
                    cast(0 as decimal(7, 2)) as return_amt,
                    cast(0 as decimal(7, 2)) as net_loss
                from {{ ref("web_sales") }}
                union all
                select
                    ws_web_site_sk as wsr_web_site_sk,
                    wr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    wr_return_amt as return_amt,
                    wr_net_loss as net_loss
                from {{ ref("web_returns") }}
                left outer join
                    {{ ref("web_sales") }}
                    on (wr_item_sk = ws_item_sk and wr_order_number = ws_order_number)
            ) salesreturns,
            {{ ref("date_dim") }},
            {{ ref("web_site") }}
        where
            date_sk = d_date_sk
            and d_date between cast('2000-08-23' as date) and cast('2000-09-06' as date)
            and wsr_web_site_sk = web_site_sk
        group by web_site_id
    )
select
    channel, id, sum(sales) as sales, sum(returns_) as returns_, sum(profit) as profit
from
    (
        select
            'store channel' as channel,
            concat('store ', s_store_id) as id,
            sales,
            returns_,
            (profit - profit_loss) as profit
        from ssr
        union all
        select
            'catalog channel' as channel,
            concat('catalog_page ', cp_catalog_page_id) as id,
            sales,
            returns_,
            (profit - profit_loss) as profit
        from csr
        union all
        select
            'web channel' as channel,
            concat('web_site ', web_site_id) as id,
            sales,
            returns_,
            (profit - profit_loss) as profit
        from wsr
    ) x
group by rollup (channel, id)
order by channel nulls first, id nulls first
limit 100
