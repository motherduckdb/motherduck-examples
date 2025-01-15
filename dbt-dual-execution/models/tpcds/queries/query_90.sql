select
    case
        when pmc = 0
        then null
        else cast(amc as decimal(15, 4)) / cast(pmc as decimal(15, 4))
    end am_pm_ratio
from
    (
        select count(*) amc
        from
            {{ ref("web_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("web_page") }}
        where
            ws_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ws_ship_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ws_web_page_sk = {{ ref("web_page") }}.wp_web_page_sk
            and {{ ref("time_dim") }}.t_hour between 8 and 8 + 1
            and {{ ref("household_demographics") }}.hd_dep_count = 6
            and {{ ref("web_page") }}.wp_char_count between 5000 and 5200
    ) at,
    (
        select count(*) pmc
        from
            {{ ref("web_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("web_page") }}
        where
            ws_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ws_ship_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ws_web_page_sk = {{ ref("web_page") }}.wp_web_page_sk
            and {{ ref("time_dim") }}.t_hour between 19 and 19 + 1
            and {{ ref("household_demographics") }}.hd_dep_count = 6
            and {{ ref("web_page") }}.wp_char_count between 5000 and 5200
    ) pt
order by am_pm_ratio
limit 100
