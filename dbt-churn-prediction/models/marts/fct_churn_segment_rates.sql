with labels as (
    select
        segment,
        prediction_window_days,
        churned
    from {{ ref('fct_customer_churn_labels') }}
),

segment_rates as (
    select
        segment,
        prediction_window_days,
        count(*) as labeled_customers,
        sum(churned) as churned_customers,
        avg(churned) as observed_churn_rate
    from labels
    group by 1, 2
)

select
    segment,
    prediction_window_days,
    labeled_customers,
    churned_customers,
    observed_churn_rate
from segment_rates
