select
    membership_id::varchar as membership_id,
    customer_id::varchar as customer_id,
    lower(status::varchar) as status,
    cast(started_at as date) as started_at,
    cast(nullif(canceled_at::varchar, '') as date) as canceled_at,
    cast(renewal_date as date) as renewal_date,
    cast(monthly_price as decimal(10, 2)) as monthly_price,
    cast(initial_plan_days as integer) as initial_plan_days,
    cast(is_auto_renew as boolean) as is_auto_renew,
    lower(initial_payment_method::varchar) as initial_payment_method,
    lower(acquisition_channel::varchar) as acquisition_channel
from {{ source('subscription_raw', 'raw_memberships') }}
