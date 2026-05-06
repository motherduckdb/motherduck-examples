with residual_counts as (
    select
        demographic_field,
        count(*) as actual_rows
    from {{ ref('fct_segment_demographic_residuals') }}
    group by 1
),

expected_counts as (
    select
        demographics.demographic_field,
        count(distinct segments.segment_id) * count(distinct demographics.demographic_value) as expected_rows
    from {{ ref('fct_household_segments') }} as segments
    inner join (
        select household_id, 'age_bracket' as demographic_field, age_bracket as demographic_value from {{ ref('stg_households') }}
        union all
        select household_id, 'income_bracket' as demographic_field, income_bracket as demographic_value from {{ ref('stg_households') }}
        union all
        select household_id, 'homeownership' as demographic_field, homeownership as demographic_value from {{ ref('stg_households') }}
        union all
        select household_id, 'composition' as demographic_field, composition as demographic_value from {{ ref('stg_households') }}
    ) as demographics
        on segments.household_id = demographics.household_id
    where demographics.demographic_value is not null
    group by 1
)

select
    expected_counts.demographic_field,
    expected_counts.expected_rows,
    coalesce(residual_counts.actual_rows, 0) as actual_rows
from expected_counts
left join residual_counts
    on expected_counts.demographic_field = residual_counts.demographic_field
where expected_counts.expected_rows != coalesce(residual_counts.actual_rows, 0)
