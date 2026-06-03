select
    payment_id::varchar as payment_id,
    customer_id::varchar as customer_id,
    membership_id::varchar as membership_id,
    cast(payment_at as timestamp) as payment_at,
    cast(payment_at as date) as payment_date,
    lower(payment_status::varchar) as payment_status,
    cast(amount as decimal(10, 2)) as amount
from {{ source('subscription_raw', 'raw_payments') }}
where customer_id is not null
  and payment_at is not null
