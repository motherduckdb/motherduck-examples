select
    scores.customer_id,
    scores.as_of_date,
    scores.segment
from {{ ref('fct_customer_churn_scores_daily') }} as scores
inner join {{ ref('fct_customer_features_daily') }} as features
    on scores.customer_id = features.customer_id
    and scores.as_of_date = features.as_of_date
where scores.segment = 'casual'
  and not features.is_eligible_for_scoring
