with memberships as (
    select
        membership_id,
        customer_id,
        status,
        started_at,
        canceled_at,
        renewal_date,
        monthly_price,
        initial_plan_days,
        is_auto_renew,
        initial_payment_method,
        acquisition_channel
    from {{ ref('stg_memberships') }}
),

episode_dates as (
    select
        *,
        cast('{{ var("churn_as_of_date") }}' as date) as observation_end_date,
        cast({{ var("member_churn_grace_period_days") }} as integer) as churn_grace_period_days,
        case
            when status = 'canceled' and canceled_at is not null then canceled_at
            when status <> 'active'
                and renewal_date + interval '{{ var("member_churn_grace_period_days") }} days' <= cast('{{ var("churn_as_of_date") }}' as date)
                then renewal_date
            else null
        end as ended_at
    from memberships
),

subscription_episodes as (
    select
        membership_id,
        customer_id,
        started_at,
        ended_at,
        observation_end_date,
        churn_grace_period_days,
        1 + date_diff('day', started_at, coalesce(ended_at, observation_end_date)) as duration_days,
        case when ended_at is null then 0 else 1 end as churned,
        case when ended_at is null then 1 else 0 end as is_censored,
        row_number() over (partition by customer_id order by started_at, membership_id) - 1 as prior_memberships,
        monthly_price,
        initial_plan_days,
        case when is_auto_renew then 1 else 0 end as is_auto_renew,
        initial_payment_method,
        acquisition_channel
    from episode_dates
)

select
    membership_id,
    customer_id,
    started_at,
    ended_at,
    observation_end_date,
    churn_grace_period_days,
    duration_days,
    churned,
    is_censored,
    prior_memberships,
    monthly_price,
    initial_plan_days,
    is_auto_renew,
    initial_payment_method,
    acquisition_channel
from subscription_episodes
