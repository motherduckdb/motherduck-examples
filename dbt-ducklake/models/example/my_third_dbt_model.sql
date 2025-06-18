{{ config(
    database="my_db",
    materialized="table"
) }}


select * from {{ ref( 'my_second_dbt_model' ) }}
