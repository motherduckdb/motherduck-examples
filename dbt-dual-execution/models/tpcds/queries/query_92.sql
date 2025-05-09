select sum(ws_ext_discount_amt) as "Excess Discount Amount"
from {{ ref("web_sales") }}, {{ ref("item") }}, {{ ref("date_dim") }}
where
    i_manufact_id = 350
    and i_item_sk = ws_item_sk
    and d_date between '2000-01-27' and cast('2000-04-26' as date)
    and d_date_sk = ws_sold_date_sk
    and ws_ext_discount_amt > (
        select 1.3 * avg(ws_ext_discount_amt)
        from {{ ref("web_sales") }}, {{ ref("date_dim") }}
        where
            ws_item_sk = i_item_sk
            and d_date between '2000-01-27' and cast('2000-04-26' as date)
            and d_date_sk = ws_sold_date_sk
    )
order by sum(ws_ext_discount_amt)
limit 100
