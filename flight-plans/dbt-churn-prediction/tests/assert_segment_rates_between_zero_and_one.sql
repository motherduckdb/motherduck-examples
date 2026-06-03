select
    segment,
    prediction_window_days,
    observed_churn_rate
from {{ ref('fct_churn_segment_rates') }}
where observed_churn_rate < 0
   or observed_churn_rate > 1
