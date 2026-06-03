select
    count(distinct ws_order_number) as "order count",
    sum(ws_ext_ship_cost) as "total shipping cost",
    sum(ws_net_profit) as "total net profit"
from
    {{ ref("web_sales") }} ws1,
    {{ ref("date_dim") }},
    {{ ref("customer_address") }},
    {{ ref("web_site") }}
where
    d_date between '1999-02-01' and cast('1999-04-02' as date)
    and ws1.ws_ship_date_sk = d_date_sk
    and ws1.ws_ship_addr_sk = ca_address_sk
    and ca_state = 'IL'
    and ws1.ws_web_site_sk = web_site_sk
    and web_company_name = 'pri'
    and exists
    (
        select *
        from {{ ref("web_sales") }} ws2
        where
            ws1.ws_order_number = ws2.ws_order_number
            and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
    )
    and not exists
    (
        select *
        from {{ ref("web_returns") }} wr1
        where ws1.ws_order_number = wr1.wr_order_number
    )
order by count(distinct ws_order_number)
limit 100
