with
    inv as (
        select
            w_warehouse_name,
            w_warehouse_sk,
            i_item_sk,
            d_moy,
            stdev,
            mean,
            case mean when 0 then null else stdev / mean end cov
        from
            (
                select
                    w_warehouse_name,
                    w_warehouse_sk,
                    i_item_sk,
                    d_moy,
                    stddev_samp(inv_quantity_on_hand) * 1.000 stdev,
                    avg(inv_quantity_on_hand) mean
                from
                    {{ ref("inventory") }},
                    {{ ref("item") }},
                    {{ ref("warehouse") }},
                    {{ ref("date_dim") }}
                where
                    inv_item_sk = i_item_sk
                    and inv_warehouse_sk = w_warehouse_sk
                    and inv_date_sk = d_date_sk
                    and d_year = 2001
                group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
            ) foo
        where case mean when 0 then 0 else stdev / mean end > 1
    )
select
    inv1.w_warehouse_sk wsk1,
    inv1.i_item_sk isk1,
    inv1.d_moy dmoy1,
    inv1.mean mean1,
    inv1.cov cov1,
    inv2.w_warehouse_sk,
    inv2.i_item_sk,
    inv2.d_moy,
    inv2.mean,
    inv2.cov
from inv inv1, inv inv2
where
    inv1.i_item_sk = inv2.i_item_sk
    and inv1.w_warehouse_sk = inv2.w_warehouse_sk
    and inv1.d_moy = 1
    and inv2.d_moy = 1 + 1
order by
    inv1.w_warehouse_sk nulls first,
    inv1.i_item_sk nulls first,
    inv1.d_moy nulls first,
    inv1.mean nulls first,
    inv1.cov nulls first,
    inv2.d_moy nulls first,
    inv2.mean nulls first,
    inv2.cov nulls first
