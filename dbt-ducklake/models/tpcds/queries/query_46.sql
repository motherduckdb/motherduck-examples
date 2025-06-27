select c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt, profit
from
    (
        select
            ss_ticket_number,
            ss_customer_sk,
            ca_city bought_city,
            sum(ss_coupon_amt) amt,
            sum(ss_net_profit) profit
        from
            {{ ref("store_sales") }},
            {{ ref("date_dim") }},
            {{ ref("store") }},
            {{ ref("household_demographics") }},
            {{ ref("customer_address") }}
        where
            {{ ref("store_sales") }}.ss_sold_date_sk = {{ ref("date_dim") }}.d_date_sk
            and {{ ref("store_sales") }}.ss_store_sk = {{ ref("store") }}.s_store_sk
            and {{ ref("store_sales") }}.ss_hdemo_sk
            = {{ ref("household_demographics") }}.hd_demo_sk
            and {{ ref("store_sales") }}.ss_addr_sk
            = {{ ref("customer_address") }}.ca_address_sk
            and (
                {{ ref("household_demographics") }}.hd_dep_count = 4
                or {{ ref("household_demographics") }}.hd_vehicle_count = 3
            )
            and {{ ref("date_dim") }}.d_dow in (6, 0)
            and {{ ref("date_dim") }}.d_year in (1999, 1999 + 1, 1999 + 2)
            and {{ ref("store") }}.s_city in ('Fairview', 'Midway')
        group by ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
    ) dn,
    {{ ref("customer") }},
    {{ ref("customer_address") }} current_addr
where
    ss_customer_sk = c_customer_sk
    and {{ ref("customer") }}.c_current_addr_sk = current_addr.ca_address_sk
    and current_addr.ca_city <> bought_city
order by
    c_last_name nulls first,
    c_first_name nulls first,
    ca_city nulls first,
    bought_city nulls first,
    ss_ticket_number nulls first
limit 100
