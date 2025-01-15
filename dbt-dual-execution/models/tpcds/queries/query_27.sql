with
    results as (
        select
            i_item_id,
            s_state,
            0 as g_state,
            ss_quantity agg1,
            ss_list_price agg2,
            ss_coupon_amt agg3,
            ss_sales_price agg4
        from
            {{ ref("store_sales") }},
            {{ ref("customer_demographics") }},
            {{ ref("date_dim") }},
            {{ ref("store") }},
            {{ ref("item") }}
        where
            ss_sold_date_sk = d_date_sk
            and ss_item_sk = i_item_sk
            and ss_store_sk = s_store_sk
            and ss_cdemo_sk = cd_demo_sk
            and cd_gender = 'M'
            and cd_marital_status = 'S'
            and cd_education_status = 'College'
            and d_year = 2002
            and s_state = 'TN'
    )
select i_item_id, s_state, g_state, agg1, agg2, agg3, agg4
from
    (
        select
            i_item_id,
            s_state,
            0 as g_state,
            avg(agg1) agg1,
            avg(agg2) agg2,
            avg(agg3) agg3,
            avg(agg4) agg4
        from results
        group by i_item_id, s_state
        union all
        select
            i_item_id,
            null as s_state,
            1 as g_state,
            avg(agg1) agg1,
            avg(agg2) agg2,
            avg(agg3) agg3,
            avg(agg4) agg4
        from results
        group by i_item_id
        union all
        select
            null as i_item_id,
            null as s_state,
            1 as g_state,
            avg(agg1) agg1,
            avg(agg2) agg2,
            avg(agg3) agg3,
            avg(agg4) agg4
        from results
    ) foo
order by i_item_id nulls first, s_state nulls first
limit 100
