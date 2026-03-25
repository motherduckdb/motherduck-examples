with
    frequent_ss_items as (
        select itemdesc, i_item_sk item_sk, d_date solddate, count(*) cnt
        from
            {{ ref("store_sales") }},
            {{ ref("date_dim") }},
            (
                select substring(i_item_desc, 1, 30) itemdesc, * from {{ ref("item") }}
            ) sq1
        where
            ss_sold_date_sk = d_date_sk
            and ss_item_sk = i_item_sk
            and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)
        group by itemdesc, i_item_sk, d_date
        having count(*) > 4
    ),
    max_store_sales as (
        select max(csales) tpcds_cmax
        from
            (
                select c_customer_sk, sum(ss_quantity * ss_sales_price) csales
                from
                    {{ ref("store_sales") }},
                    {{ ref("customer") }},
                    {{ ref("date_dim") }}
                where
                    ss_customer_sk = c_customer_sk
                    and ss_sold_date_sk = d_date_sk
                    and d_year in (2000, 2000 + 1, 2000 + 2, 2000 + 3)
                group by c_customer_sk
            ) sq2
    ),
    best_ss_customer as (
        select c_customer_sk, sum(ss_quantity * ss_sales_price) ssales
        from {{ ref("store_sales") }}, {{ ref("customer") }}, max_store_sales
        where ss_customer_sk = c_customer_sk
        group by c_customer_sk
        having sum(ss_quantity * ss_sales_price) > (50 / 100.0) * max(tpcds_cmax)
    )
select c_last_name, c_first_name, sales
from
    (
        select c_last_name, c_first_name, sum(cs_quantity * cs_list_price) sales
        from
            {{ ref("catalog_sales") }},
            {{ ref("customer") }},
            {{ ref("date_dim") }},
            frequent_ss_items,
            best_ss_customer
        where
            d_year = 2000
            and d_moy = 2
            and cs_sold_date_sk = d_date_sk
            and cs_item_sk = item_sk
            and cs_bill_customer_sk = best_ss_customer.c_customer_sk
            and cs_bill_customer_sk = {{ ref("customer") }}.c_customer_sk
        group by c_last_name, c_first_name
        union all
        select c_last_name, c_first_name, sum(ws_quantity * ws_list_price) sales
        from
            {{ ref("web_sales") }},
            {{ ref("customer") }},
            {{ ref("date_dim") }},
            frequent_ss_items,
            best_ss_customer
        where
            d_year = 2000
            and d_moy = 2
            and ws_sold_date_sk = d_date_sk
            and ws_item_sk = item_sk
            and ws_bill_customer_sk = best_ss_customer.c_customer_sk
            and ws_bill_customer_sk = {{ ref("customer") }}.c_customer_sk
        group by c_last_name, c_first_name
    ) sq3
order by c_last_name nulls first, c_first_name nulls first, sales nulls first
limit 100
