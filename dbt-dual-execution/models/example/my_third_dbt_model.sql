{{ config( materialized="table")}}

select * from {{ ref( 'my_second_dbt_model' ) }}
