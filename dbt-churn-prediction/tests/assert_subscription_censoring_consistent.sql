select
    membership_id,
    customer_id,
    ended_at,
    churned,
    is_censored
from {{ ref('fct_subscription_history') }}
where (ended_at is null and (churned != 0 or is_censored != 1))
   or (ended_at is not null and (churned != 1 or is_censored != 0))
