select *
from
    (
        select count(*) h8_30_to_9
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 8
            and {{ ref("time_dim") }}.t_minute >= 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s1,
    (
        select count(*) h9_to_9_30
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 9
            and {{ ref("time_dim") }}.t_minute < 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s2,
    (
        select count(*) h9_30_to_10
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 9
            and {{ ref("time_dim") }}.t_minute >= 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s3,
    (
        select count(*) h10_to_10_30
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 10
            and {{ ref("time_dim") }}.t_minute < 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s4,
    (
        select count(*) h10_30_to_11
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 10
            and {{ ref("time_dim") }}.t_minute >= 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s5,
    (
        select count(*) h11_to_11_30
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 11
            and {{ ref("time_dim") }}.t_minute < 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s6,
    (
        select count(*) h11_30_to_12
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 11
            and {{ ref("time_dim") }}.t_minute >= 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s7,
    (
        select count(*) h12_to_12_30
        from
            {{ ref("store_sales") }},
            {{ ref("household_demographics") }},
            {{ ref("time_dim") }},
            {{ ref("store") }}
        where
            ss_sold_time_sk = {{ ref("time_dim") }}.t_time_sk
            and ss_hdemo_sk = {{ ref("household_demographics") }}.hd_demo_sk
            and ss_store_sk = s_store_sk
            and {{ ref("time_dim") }}.t_hour = 12
            and {{ ref("time_dim") }}.t_minute < 30
            and (
                (
                    {{ ref("household_demographics") }}.hd_dep_count = 4
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 4 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 2
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 2 + 2
                )
                or (
                    {{ ref("household_demographics") }}.hd_dep_count = 0
                    and {{ ref("household_demographics") }}.hd_vehicle_count <= 0 + 2
                )
            )
            and {{ ref("store") }}.s_store_name = 'ese'
    ) s8
