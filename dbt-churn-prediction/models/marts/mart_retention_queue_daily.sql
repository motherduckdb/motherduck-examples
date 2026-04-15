with scored as (
    select
        customer_id,
        as_of_date,
        region_id,
        customer_name,
        marketing_opt_in,
        segment,
        membership_age_days,
        prior_memberships,
        is_auto_renew,
        days_since_last_event,
        events_30d,
        failed_payments_30d,
        complaints_60d,
        avg_satisfaction_60d,
        churn_probability
    from {{ ref('fct_customer_churn_scores_daily') }}
),

reasoned as (
    select
        customer_id,
        as_of_date,
        region_id,
        customer_name,
        marketing_opt_in,
        segment,
        membership_age_days,
        prior_memberships,
        is_auto_renew,
        days_since_last_event,
        events_30d,
        failed_payments_30d,
        complaints_60d,
        avg_satisfaction_60d,
        churn_probability,
        case
            when segment = 'member' and failed_payments_30d > 0 then 'failed membership payment'
            when segment = 'member' and is_auto_renew = 0 then 'manual renewal risk'
            when complaints_60d > 0 then 'recent complaint'
            when days_since_last_event >= 45 then 'long engagement gap'
            when events_30d = 0 then 'no events in last 30 days'
            else 'elevated churn score'
        end as top_reason_1,
        case
            when complaints_60d > 0 and top_reason_1 <> 'recent complaint' then 'recent complaint'
            when segment = 'member' and prior_memberships > 0 then 'reactivated subscriber'
            when days_since_last_event >= 30 and top_reason_1 <> 'long engagement gap' then 'event frequency dropped'
            when avg_satisfaction_60d is not null and avg_satisfaction_60d < 7 then 'low recent satisfaction'
            else null
        end as top_reason_2,
        case
            when marketing_opt_in then 'customer can receive marketing'
            else 'customer is not opted into marketing'
        end as top_reason_3
    from scored
),

actions as (
    select
        as_of_date,
        row_number() over (partition by as_of_date order by churn_probability desc, customer_id) as queue_rank,
        customer_id,
        customer_name,
        region_id,
        segment,
        churn_probability,
        case
            when churn_probability >= 0.70 then 'high'
            when churn_probability >= 0.45 then 'medium'
            else 'low'
        end as risk_band,
        top_reason_1,
        top_reason_2,
        top_reason_3,
        case
            when segment = 'member' and failed_payments_30d > 0 then 'send payment recovery message'
            when complaints_60d > 0 then 'assign service recovery follow-up'
            when segment = 'member' and is_auto_renew = 0 then 'send renewal reminder'
            when segment = 'casual' then 'send win-back offer'
            else 'send retention check-in'
        end as recommended_action,
        case
            when segment = 'member' and failed_payments_30d > 0 then 'payment update link'
            when complaints_60d > 0 then 'manager apology'
            when segment = 'member' and is_auto_renew = 0 then 'renewal reminder'
            when segment = 'casual' then 'targeted win-back offer'
            else 'loyalty reminder'
        end as offer_type
    from reasoned
    where churn_probability >= 0.35
)

select
    as_of_date,
    queue_rank,
    customer_id,
    customer_name,
    region_id,
    segment,
    churn_probability,
    risk_band,
    top_reason_1,
    top_reason_2,
    top_reason_3,
    recommended_action,
    offer_type
from actions
