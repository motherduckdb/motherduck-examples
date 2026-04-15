select
    customer_id::varchar as customer_id,
    region_id::varchar as region_id,
    customer_name::varchar as customer_name,
    cast(signup_date as date) as signup_date,
    cast(marketing_opt_in as boolean) as marketing_opt_in
from {{ source('subscription_raw', 'raw_customers') }}
