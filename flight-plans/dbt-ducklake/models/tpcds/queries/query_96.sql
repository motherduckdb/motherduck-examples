select count(*)
from
    {{ ref("store_sales") }},
    {{ ref("household_demographics") }},
    {{ ref("time_dim") }},
    {{ ref("store") }}
where
    ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
    and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
    and ss_store_sk = s_store_sk
    and {{ ref("time_dim") }}.t_hour = 20
    and {{ ref("time_dim") }}.t_minute >= 30
    and {{ ref("household_demographics") }}.hd_dep_count = 7
    and {{ ref("store") }}.s_store_name = 'ese'
order by count(*)
limit 100
