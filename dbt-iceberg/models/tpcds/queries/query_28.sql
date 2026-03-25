select *
from
    (
        select
            avg(ss_list_price) b1_lp,
            count(ss_list_price) b1_cnt,
            count(distinct ss_list_price) b1_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 0 and 5
            and (
                ss_list_price between 8 and 8 + 10
                or ss_coupon_amt between 459 and 459 + 1000
                or ss_wholesale_cost between 57 and 57 + 20
            )
    ) b1,
    (
        select
            avg(ss_list_price) b2_lp,
            count(ss_list_price) b2_cnt,
            count(distinct ss_list_price) b2_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 6 and 10
            and (
                ss_list_price between 90 and 90 + 10
                or ss_coupon_amt between 2323 and 2323 + 1000
                or ss_wholesale_cost between 31 and 31 + 20
            )
    ) b2,
    (
        select
            avg(ss_list_price) b3_lp,
            count(ss_list_price) b3_cnt,
            count(distinct ss_list_price) b3_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 11 and 15
            and (
                ss_list_price between 142 and 142 + 10
                or ss_coupon_amt between 12214 and 12214 + 1000
                or ss_wholesale_cost between 79 and 79 + 20
            )
    ) b3,
    (
        select
            avg(ss_list_price) b4_lp,
            count(ss_list_price) b4_cnt,
            count(distinct ss_list_price) b4_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 16 and 20
            and (
                ss_list_price between 135 and 135 + 10
                or ss_coupon_amt between 6071 and 6071 + 1000
                or ss_wholesale_cost between 38 and 38 + 20
            )
    ) b4,
    (
        select
            avg(ss_list_price) b5_lp,
            count(ss_list_price) b5_cnt,
            count(distinct ss_list_price) b5_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 21 and 25
            and (
                ss_list_price between 122 and 122 + 10
                or ss_coupon_amt between 836 and 836 + 1000
                or ss_wholesale_cost between 17 and 17 + 20
            )
    ) b5,
    (
        select
            avg(ss_list_price) b6_lp,
            count(ss_list_price) b6_cnt,
            count(distinct ss_list_price) b6_cntd
        from {{ ref("store_sales") }}
        where
            ss_quantity between 26 and 30
            and (
                ss_list_price between 154 and 154 + 10
                or ss_coupon_amt between 7326 and 7326 + 1000
                or ss_wholesale_cost between 7 and 7 + 20
            )
    ) b6
limit 100
