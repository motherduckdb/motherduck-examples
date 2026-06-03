select
    event_id::varchar as event_id,
    customer_id::varchar as customer_id,
    region_id::varchar as region_id,
    cast(event_at as timestamp) as event_at,
    cast(event_at as date) as event_date,
    lower(event_type::varchar) as event_type,
    cast(net_amount as decimal(10, 2)) as net_amount,
    cast(used_coupon as boolean) as used_coupon,
    cast(satisfaction_score as integer) as satisfaction_score,
    cast(complaint_flag as boolean) as complaint_flag
from {{ source('subscription_raw', 'raw_usage_events') }}
where customer_id is not null
  and event_at is not null
