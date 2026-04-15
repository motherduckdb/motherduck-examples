select
    customer_id,
    as_of_date,
    churn_probability
from {{ ref('fct_customer_churn_scores_daily') }}
where churn_probability < 0
   or churn_probability > 1
