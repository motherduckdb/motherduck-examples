{{config(
    database = "catalog"
)}}

select * from {{ source('tpch', 'customer') }}