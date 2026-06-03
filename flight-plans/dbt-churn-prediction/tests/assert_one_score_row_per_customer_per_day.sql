select
    customer_id,
    as_of_date,
    count(*) as row_count
from {{ ref('fct_customer_churn_scores_daily') }}
group by 1, 2
having count(*) != 1
