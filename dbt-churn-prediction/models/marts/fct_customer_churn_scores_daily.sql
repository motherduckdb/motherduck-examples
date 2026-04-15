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

rule_matches as (
    select customer_id, as_of_date, segment, 'failed_payment' as rule_name
    from feature_rows
    where segment = 'member' and failed_payments_30d > 0

    union all

    select customer_id, as_of_date, segment, 'manual_renewal' as rule_name
    from feature_rows
    where segment = 'member' and is_auto_renew = 0

    union all

    select customer_id, as_of_date, segment, 'recent_complaint' as rule_name
    from feature_rows
    where complaints_60d > 0

    union all

    select customer_id, as_of_date, segment, 'long_engagement_gap' as rule_name
    from feature_rows
    where days_since_last_event >= 45

    union all

    select customer_id, as_of_date, segment, 'no_recent_events' as rule_name
    from feature_rows
    where events_30d = 0

    union all

    select customer_id, as_of_date, segment, 'declining_event_frequency' as rule_name
    from feature_rows
    where events_30d = 0 and events_90d > 0

    union all

    select customer_id, as_of_date, segment, 'low_recent_satisfaction' as rule_name
    from feature_rows
    where avg_satisfaction_60d is not null and avg_satisfaction_60d < 7

    union all

    select customer_id, as_of_date, segment, 'short_member_tenure' as rule_name
    from feature_rows
    where segment = 'member' and membership_age_days < 90

    union all

    select customer_id, as_of_date, segment, 'reactivated_member' as rule_name
    from feature_rows
    where segment = 'member' and prior_memberships > 0

    union all

    select customer_id, as_of_date, segment, 'not_marketable' as rule_name
    from feature_rows
    where not marketing_opt_in
),

rule_details as (
    select
        rule_matches.customer_id,
        rule_matches.as_of_date,
        rule_matches.rule_name,
        rules.risk_points,
        rules.reason,
        rules.recommended_action,
        rules.offer_type,
        row_number() over (
            partition by rule_matches.customer_id, rule_matches.as_of_date
            order by rules.risk_points desc, rule_matches.rule_name
        ) as reason_rank
    from rule_matches
    inner join {{ ref('churn_risk_rules') }} as rules
        on rule_matches.rule_name = rules.rule_name
        and (rules.segment = rule_matches.segment or rules.segment = 'all')
),

rule_summary as (
    select
        customer_id,
        as_of_date,
        count(*) as matched_rule_count,
        sum(risk_points) as rule_risk_points,
        string_agg(rule_name, ', ' order by risk_points desc, rule_name) as matched_rules,
        max(case when reason_rank = 1 then reason end) as top_reason_1,
        max(case when reason_rank = 2 then reason end) as top_reason_2,
        max(case when reason_rank = 3 then reason end) as top_reason_3,
        max(case when reason_rank = 1 then recommended_action end) as recommended_action,
        max(case when reason_rank = 1 then offer_type end) as offer_type
    from rule_details
    group by 1, 2
),

segment_rates as (
    select
        segment,
        prediction_window_days,
        labeled_customers,
        churned_customers,
        observed_churn_rate
    from {{ ref('fct_churn_segment_rates') }}
),

scored as (
    select
        feature_rows.customer_id,
        feature_rows.as_of_date,
        feature_rows.region_id,
        feature_rows.customer_name,
        feature_rows.marketing_opt_in,
        feature_rows.segment,
        case when feature_rows.segment = 'member' then 30 else 60 end as prediction_window_days,
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
        coalesce(segment_rates.labeled_customers, 0) as labeled_customers,
        coalesce(segment_rates.churned_customers, 0) as churned_customers,
        coalesce(segment_rates.observed_churn_rate, 0.0) as observed_churn_rate,
        coalesce(rule_summary.matched_rule_count, 0) as matched_rule_count,
        coalesce(rule_summary.rule_risk_points, 0) as rule_risk_points,
        least(
            100,
            coalesce(rule_summary.rule_risk_points, 0) + cast(round(coalesce(segment_rates.observed_churn_rate, 0.0) * 30, 0) as integer)
        ) as risk_score,
        coalesce(rule_summary.matched_rules, 'no risk rules matched') as matched_rules,
        coalesce(rule_summary.top_reason_1, 'historical segment churn rate') as top_reason_1,
        rule_summary.top_reason_2,
        rule_summary.top_reason_3,
        coalesce(rule_summary.recommended_action, 'monitor segment trend') as recommended_action,
        coalesce(rule_summary.offer_type, 'no offer') as offer_type
    from feature_rows
    left join segment_rates
        on feature_rows.segment = segment_rates.segment
        and case when feature_rows.segment = 'member' then 30 else 60 end = segment_rates.prediction_window_days
    left join rule_summary
        on feature_rows.customer_id = rule_summary.customer_id
        and feature_rows.as_of_date = rule_summary.as_of_date
)

select
    customer_id,
    as_of_date,
    region_id,
    customer_name,
    marketing_opt_in,
    segment,
    prediction_window_days,
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
    labeled_customers,
    churned_customers,
    observed_churn_rate,
    matched_rule_count,
    rule_risk_points,
    risk_score,
    matched_rules,
    top_reason_1,
    top_reason_2,
    top_reason_3,
    recommended_action,
    offer_type
from scored
