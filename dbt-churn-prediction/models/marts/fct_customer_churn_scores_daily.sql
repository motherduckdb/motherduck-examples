with feature_rows as (
    select
        customer_id,
        as_of_date,
        region_id,
        customer_name,
        marketing_opt_in,
        segment,
        is_active_member,
        membership_age_days,
        prior_memberships,
        is_auto_renew,
        days_since_last_event,
        events_30d,
        events_60d,
        events_90d,
        spend_90d,
        failed_payments_30d,
        complaints_60d,
        avg_satisfaction_60d
    from {{ ref('fct_customer_features_daily') }}
    where is_eligible_for_scoring
),

weights as (
    select
        segment,
        max(case when feature_name = 'intercept' then coefficient end) as intercept,
        max(case when feature_name = 'days_since_last_event' then coefficient end) as days_since_last_event_weight,
        max(case when feature_name = 'events_30d' then coefficient end) as events_30d_weight,
        max(case when feature_name = 'spend_90d' then coefficient end) as spend_90d_weight,
        max(case when feature_name = 'failed_payments_30d' then coefficient end) as failed_payments_30d_weight,
        max(case when feature_name = 'complaints_60d' then coefficient end) as complaints_60d_weight,
        max(case when feature_name = 'is_active_member' then coefficient end) as is_active_member_weight,
        max(case when feature_name = 'membership_age_days' then coefficient end) as membership_age_days_weight,
        max(case when feature_name = 'prior_memberships' then coefficient end) as prior_memberships_weight,
        max(case when feature_name = 'is_auto_renew' then coefficient end) as is_auto_renew_weight
    from {{ ref('churn_score_weights') }}
    group by 1
),

logits as (
    select
        feature_rows.customer_id,
        feature_rows.as_of_date,
        feature_rows.region_id,
        feature_rows.customer_name,
        feature_rows.marketing_opt_in,
        feature_rows.segment,
        feature_rows.membership_age_days,
        feature_rows.prior_memberships,
        feature_rows.is_auto_renew,
        feature_rows.days_since_last_event,
        feature_rows.events_30d,
        feature_rows.events_60d,
        feature_rows.events_90d,
        feature_rows.spend_90d,
        feature_rows.failed_payments_30d,
        feature_rows.complaints_60d,
        feature_rows.avg_satisfaction_60d,
        (
            weights.intercept
            + weights.days_since_last_event_weight * feature_rows.days_since_last_event
            + weights.events_30d_weight * feature_rows.events_30d
            + weights.spend_90d_weight * feature_rows.spend_90d
            + weights.failed_payments_30d_weight * feature_rows.failed_payments_30d
            + weights.complaints_60d_weight * feature_rows.complaints_60d
            + weights.is_active_member_weight * feature_rows.is_active_member
            + weights.membership_age_days_weight * feature_rows.membership_age_days
            + weights.prior_memberships_weight * feature_rows.prior_memberships
            + weights.is_auto_renew_weight * feature_rows.is_auto_renew
        ) as churn_logit
    from feature_rows
    inner join weights
        on feature_rows.segment = weights.segment
)

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
    events_60d,
    events_90d,
    spend_90d,
    failed_payments_30d,
    complaints_60d,
    avg_satisfaction_60d,
    churn_logit,
    1.0 / (1.0 + exp(-least(greatest(churn_logit, -20), 20))) as churn_probability
from logits
