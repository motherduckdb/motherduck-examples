{{ config(
    database="jdw_dev",
    schema="jdw_tpcds",
    materialized="table"
) }}

select
    w_substr,
    sm_type,
    lower(cc_name) cc_name_lower,
    sum(
        case when (cs_ship_date_sk - cs_sold_date_sk <= 30) then 1 else 0 end
    ) as "30 days",
    sum(
        case
            when
                (cs_ship_date_sk - cs_sold_date_sk > 30)
                and (cs_ship_date_sk - cs_sold_date_sk <= 60)
            then 1
            else 0
        end
    ) as "31-60 days",
    sum(
        case
            when
                (cs_ship_date_sk - cs_sold_date_sk > 60)
                and (cs_ship_date_sk - cs_sold_date_sk <= 90)
            then 1
            else 0
        end
    ) as "61-90 days",
    sum(
        case
            when
                (cs_ship_date_sk - cs_sold_date_sk > 90)
                and (cs_ship_date_sk - cs_sold_date_sk <= 120)
            then 1
            else 0
        end
    ) as "91-120 days",
    sum(
        case when (cs_ship_date_sk - cs_sold_date_sk > 120) then 1 else 0 end
    ) as ">120 days"
from
    {{ ref("catalog_sales") }},
    (
        select substring(w_warehouse_name, 1, 20) w_substr, *
        from {{ ref("warehouse") }}
    ) as sq1,
    {{ ref("ship_mode") }},
    {{ ref("call_center") }},
    {{ ref("date_dim") }}
where
    d_month_seq between 1200 and 1200 + 11
    and cs_ship_date_sk = d_date_sk
    and cs_warehouse_sk = w_warehouse_sk
    and cs_ship_mode_sk = sm_ship_mode_sk
    and cs_call_center_sk = cc_call_center_sk
group by w_substr, sm_type, cc_name
order by w_substr nulls first, sm_type nulls first, cc_name_lower nulls first
limit 100
