select
    c_last_name, c_first_name, substring(s_city, 1, 30), ss_ticket_number, amt, profit
from
    (
        select
            ss_ticket_number,
            ss_customer_sk,
            {{ ref("store") }}.s_city,
            sum(ss_coupon_amt) amt,
            sum(ss_net_profit) profit
        from
            {{ ref("store_sales") }},
            {{ ref("date_dim") }},
            {{ ref("store") }},
            {{ ref("household_demographics") }}
        where
            {{ ref("store_sales") }}.ss_sold_date_sk = {{ ref("date_dim") }}.d_date_sk
            and {{ ref("store_sales") }}.ss_store_sk = {{ ref("store") }}.s_store_sk
            and {{ ref("store_sales") }}.ss_hdemo_sk
            = {{ ref("household_demographics") }}.hd_demo_sk
            and (
                {{ ref("household_demographics") }}.hd_dep_count = 6
                or {{ ref("household_demographics") }}.hd_vehicle_count > 2
            )
            and {{ ref("date_dim") }}.d_dow = 1
            and {{ ref("date_dim") }}.d_year in (1999, 1999 + 1, 1999 + 2)
            and {{ ref("store") }}.s_number_employees between 200 and 295
        group by ss_ticket_number, ss_customer_sk, ss_addr_sk, {{ ref("store") }}.s_city
    ) ms,
    {{ ref("customer") }}
where ss_customer_sk = c_customer_sk
order by
    c_last_name nulls first,
    c_first_name nulls first,
    substring(s_city, 1, 30) nulls first,
    profit nulls first,
    ss_ticket_number
limit 100
