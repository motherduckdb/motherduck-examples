select
    i_item_id,
    avg(cs_quantity) agg1,
    avg(cs_list_price) agg2,
    avg(cs_coupon_amt) agg3,
    avg(cs_sales_price) agg4
from
    {{ ref("catalog_sales") }},
    {{ ref("customer_demographics") }},
    {{ ref("date_dim") }},
    {{ ref("item") }},
    {{ ref("promotion") }}
where
    cs_sold_date_sk = d_date_sk
    and cs_item_sk = i_item_sk
    and cs_bill_cdemo_sk = cd_demo_sk
    and cs_promo_sk = p_promo_sk
    and cd_gender = 'M'
    and cd_marital_status = 'S'
    and cd_education_status = 'College'
    and (p_channel_email = 'N' or p_channel_event = 'N')
    and d_year = 2000
group by i_item_id
order by i_item_id
limit 100
