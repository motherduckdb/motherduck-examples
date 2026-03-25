select
    w_substr,
    sm_type,
    web_name,
    sum(
        case when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1 else 0 end
    ) as "30 days",
    sum(
        case
            when
                (ws_ship_date_sk - ws_sold_date_sk > 30)
                and (ws_ship_date_sk - ws_sold_date_sk <= 60)
            then 1
            else 0
        end
    ) as "31-60 days",
    sum(
        case
            when
                (ws_ship_date_sk - ws_sold_date_sk > 60)
                and (ws_ship_date_sk - ws_sold_date_sk <= 90)
            then 1
            else 0
        end
    ) as "61-90 days",
    sum(
        case
            when
                (ws_ship_date_sk - ws_sold_date_sk > 90)
                and (ws_ship_date_sk - ws_sold_date_sk <= 120)
            then 1
            else 0
        end
    ) as "91-120 days",
    sum(
        case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0 end
    ) as ">120 days"
from
    {{ ref("web_sales") }},
    (
        select substring(w_warehouse_name, 1, 20) w_substr, *
        from {{ ref("warehouse") }}
    ) sq1,
    {{ ref("ship_mode") }},
    {{ ref("web_site") }},
    {{ ref("date_dim") }}
where
    d_month_seq between 1200 and 1200 + 11
    and ws_ship_date_sk = d_date_sk
    and ws_warehouse_sk = w_warehouse_sk
    and ws_ship_mode_sk = sm_ship_mode_sk
    and ws_web_site_sk = web_site_sk
group by w_substr, sm_type, web_name
order by 1 nulls first, 2 nulls first, 3 nulls first
limit 100
