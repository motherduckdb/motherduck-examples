select *
from
    (
        select
            w_warehouse_name,
            i_item_id,
            sum(
                case
                    when (cast(d_date as date) < cast('2000-03-11' as date))
                    then inv_quantity_on_hand
                    else 0
                end
            ) as inv_before,
            sum(
                case
                    when (cast(d_date as date) >= cast('2000-03-11' as date))
                    then inv_quantity_on_hand
                    else 0
                end
            ) as inv_after
        from
            {{ ref("inventory") }},
            {{ ref("warehouse") }},
            {{ ref("item") }},
            {{ ref("date_dim") }}
        where
            i_current_price between 0.99 and 1.49
            and i_item_sk = inv_item_sk
            and inv_warehouse_sk = w_warehouse_sk
            and inv_date_sk = d_date_sk
            and d_date between cast('2000-02-10' as date) and cast('2000-04-10' as date)
        group by w_warehouse_name, i_item_id
    ) x
where
    (case when inv_before > 0 then (inv_after * 1.000) / inv_before else null end)
    between 2.000 / 3.000 and 3.000 / 2.000
order by w_warehouse_name nulls first, i_item_id nulls first
limit 100
