select i_item_id, i_item_desc, i_current_price
from
    {{ ref("item") }},
    {{ ref("inventory") }},
    {{ ref("date_dim") }},
    {{ ref("store_sales") }}
where
    i_current_price between 62 and 62 + 30
    and inv_item_sk = i_item_sk
    and d_date_sk = inv_date_sk
    and d_date between cast('2000-05-25' as date) and cast('2000-07-24' as date)
    and i_manufact_id in (129, 270, 821, 423)
    and inv_quantity_on_hand between 100 and 500
    and ss_item_sk = i_item_sk
group by i_item_id, i_item_desc, i_current_price
order by i_item_id
limit 100
