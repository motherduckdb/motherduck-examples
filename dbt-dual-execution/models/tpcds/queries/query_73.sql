select
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    ss_ticket_number,
    cnt
from
    (
        select ss_ticket_number, ss_customer_sk, count(*) cnt
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
            and {{ ref("date_dim") }}.d_dom between 1 and 2
            and (
                {{ ref("household_demographics") }}.hd_buy_potential = 'Unknown'
                or {{ ref("household_demographics") }}.hd_buy_potential = '>10000'
            )
            and {{ ref("household_demographics") }}.hd_vehicle_count > 0
            and case
                when {{ ref("household_demographics") }}.hd_vehicle_count > 0
                then
                    ({{ ref("household_demographics") }}.hd_dep_count * 1.000)
                    / {{ ref("household_demographics") }}.hd_vehicle_count
                else null
            end
            > 1
            and {{ ref("date_dim") }}.d_year in (1999, 1999 + 1, 1999 + 2)
            and {{ ref("store") }}.s_county
            in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')
        group by ss_ticket_number, ss_customer_sk
    ) dj,
    {{ ref("customer") }}
where ss_customer_sk = c_customer_sk and cnt between 1 and 5
order by cnt desc, c_last_name asc
