select
    customer_id,
    as_of_date,
    risk_score
from {{ ref('fct_customer_churn_scores_daily') }}
where risk_score < 0
   or risk_score > 100
