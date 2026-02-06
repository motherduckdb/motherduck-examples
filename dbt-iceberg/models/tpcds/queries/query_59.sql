with
    wss as (
        select
            d_week_seq,
            ss_store_sk,
            sum(
                case when (d_day_name = 'Sunday') then ss_sales_price else null end
            ) sun_sales,
            sum(
                case when (d_day_name = 'Monday') then ss_sales_price else null end
            ) mon_sales,
            sum(
                case when (d_day_name = 'Tuesday') then ss_sales_price else null end
            ) tue_sales,
            sum(
                case when (d_day_name = 'Wednesday') then ss_sales_price else null end
            ) wed_sales,
            sum(
                case when (d_day_name = 'Thursday') then ss_sales_price else null end
            ) thu_sales,
            sum(
                case when (d_day_name = 'Friday') then ss_sales_price else null end
            ) fri_sales,
            sum(
                case when (d_day_name = 'Saturday') then ss_sales_price else null end
            ) sat_sales
        from {{ ref("store_sales") }}, {{ ref("date_dim") }}
        where d_date_sk = ss_sold_date_sk
        group by d_week_seq, ss_store_sk
    )
select
    s_store_name1,
    s_store_id1,
    d_week_seq1,
    sun_sales1 / sun_sales2 as sun_sales_ratio,
    mon_sales1 / mon_sales2 as mon_sales_ratio,
    tue_sales1 / tue_sales2 as tue_sales_ratio,
    wed_sales1 / wed_sales2 as wed_sales_ratio,
    thu_sales1 / thu_sales2 as thu_sales_ratio,
    fri_sales1 / fri_sales2 as fri_sales_ratio,
    sat_sales1 / sat_sales2 as sat_sales_ratio
from
    (
        select
            s_store_name s_store_name1,
            wss.d_week_seq d_week_seq1,
            s_store_id s_store_id1,
            sun_sales sun_sales1,
            mon_sales mon_sales1,
            tue_sales tue_sales1,
            wed_sales wed_sales1,
            thu_sales thu_sales1,
            fri_sales fri_sales1,
            sat_sales sat_sales1
        from wss, {{ ref("store") }}, {{ ref("date_dim") }} d
        where
            d.d_week_seq = wss.d_week_seq
            and ss_store_sk = s_store_sk
            and d_month_seq between 1212 and 1212 + 11
    ) y,
    (
        select
            s_store_name s_store_name2,
            wss.d_week_seq d_week_seq2,
            s_store_id s_store_id2,
            sun_sales sun_sales2,
            mon_sales mon_sales2,
            tue_sales tue_sales2,
            wed_sales wed_sales2,
            thu_sales thu_sales2,
            fri_sales fri_sales2,
            sat_sales sat_sales2
        from wss, {{ ref("store") }}, {{ ref("date_dim") }} d
        where
            d.d_week_seq = wss.d_week_seq
            and ss_store_sk = s_store_sk
            and d_month_seq between 1212 + 12 and 1212 + 23
    ) x
where s_store_id1 = s_store_id2 and d_week_seq1 = d_week_seq2 - 52
order by s_store_name1 nulls first, s_store_id1 nulls first, d_week_seq1 nulls first
limit 100
