select
    c_customer_id as customer_id,
    concat(
        concat(coalesce(c_last_name, ''), ', '), coalesce(c_first_name, '')
    ) as customername
from
    {{ ref("customer") }},
    {{ ref("customer_address") }},
    {{ ref("customer_demographics") }},
    {{ ref("household_demographics") }},
    {{ ref("income_band") }},
    {{ ref("store_returns") }}
where
    ca_city = 'Edgewood'
    and c_current_addr_sk = ca_address_sk
    and ib_lower_bound >= 38128
    and ib_upper_bound <= 38128 + 50000
    and ib_income_band_sk = hd_income_band_sk
    and cd_demo_sk = c_current_cdemo_sk
    and hd_demo_sk = c_current_hdemo_sk
    and sr_cdemo_sk = cd_demo_sk
order by c_customer_id nulls first
limit 100
