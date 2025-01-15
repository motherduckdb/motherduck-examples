select count(*)
from
    (
        (
            select distinct c_last_name, c_first_name, d_date
            from {{ ref("store_sales") }}, {{ ref("date_dim") }}, {{ ref("customer") }}
            where
                {{ ref("store_sales") }}.ss_sold_date_sk
                = {{ ref("date_dim") }}.d_date_sk
                and {{ ref("store_sales") }}.ss_customer_sk
                = {{ ref("customer") }}.c_customer_sk
                and d_month_seq between 1200 and 1200 + 11
        )
        except
        (
            select distinct c_last_name, c_first_name, d_date
            from
                {{ ref("catalog_sales") }}, {{ ref("date_dim") }}, {{ ref("customer") }}
            where
                {{ ref("catalog_sales") }}.cs_sold_date_sk
                = {{ ref("date_dim") }}.d_date_sk
                and {{ ref("catalog_sales") }}.cs_bill_customer_sk
                = {{ ref("customer") }}.c_customer_sk
                and d_month_seq between 1200 and 1200 + 11
        )
        except
        (
            select distinct c_last_name, c_first_name, d_date
            from {{ ref("web_sales") }}, {{ ref("date_dim") }}, {{ ref("customer") }}
            where
                {{ ref("web_sales") }}.ws_sold_date_sk = {{ ref("date_dim") }}.d_date_sk
                and {{ ref("web_sales") }}.ws_bill_customer_sk
                = {{ ref("customer") }}.c_customer_sk
                and d_month_seq between 1200 and 1200 + 11
        )
    ) cool_cust
