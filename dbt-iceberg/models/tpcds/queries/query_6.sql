select a.ca_state state, count(*) cnt
from
    {{ ref("customer_address") }} a,
    {{ ref("customer") }} c,
    {{ ref("store_sales") }} s,
    {{ ref("date_dim") }} d,
    {{ ref("item") }} i
where
    a.ca_address_sk = c.c_current_addr_sk
    and c.c_customer_sk = s.ss_customer_sk
    and s.ss_sold_date_sk = d.d_date_sk
    and s.ss_item_sk = i.i_item_sk
    and d.d_month_seq = (
        select distinct (d_month_seq)
        from {{ ref("date_dim") }}
        where d_year = 2001 and d_moy = 1
    )
    and i.i_current_price
    > 1.2
    * (
        select avg(j.i_current_price)
        from {{ ref("item") }} j
        where j.i_category = i.i_category
    )
group by a.ca_state
having count(*) >= 10
order by cnt nulls first, a.ca_state nulls first
limit 100
