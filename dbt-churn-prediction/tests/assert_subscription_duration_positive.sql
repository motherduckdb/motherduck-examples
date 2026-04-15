select
    membership_id,
    customer_id,
    duration_days
from {{ ref('fct_subscription_history') }}
where duration_days <= 0
