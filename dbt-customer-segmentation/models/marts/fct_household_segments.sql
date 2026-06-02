with ranked_distances as (
    select
        household_id,
        segment_id,
        segment_name,
        scored_feature_count,
        segment_distance,
        row_number() over (
            partition by household_id
            order by segment_distance, segment_id
        ) as distance_rank
    from {{ ref('fct_household_segment_distances') }}
),

assigned as (
    select
        household_id,
        max(case when distance_rank = 1 then segment_id end) as segment_id,
        max(case when distance_rank = 1 then segment_name end) as segment_name,
        max(case when distance_rank = 1 then segment_distance end) as segment_distance,
        max(case when distance_rank = 2 then segment_id end) as alternative_segment_id,
        max(case when distance_rank = 2 then segment_name end) as alternative_segment_name,
        max(case when distance_rank = 2 then segment_distance end) as alternative_segment_distance,
        max(scored_feature_count) as scored_feature_count
    from ranked_distances
    group by 1
)

select
    assigned.household_id,
    cast('{{ var("segmentation_reference_day") }}' as integer) as segmentation_reference_day,
    assigned.segment_id,
    assigned.segment_name,
    playbook.segment_label,
    playbook.segment_description,
    playbook.recommended_action,
    playbook.recommended_offer,
    assigned.segment_distance,
    assigned.alternative_segment_id,
    assigned.alternative_segment_name,
    assigned.alternative_segment_distance,
    greatest(
        0.0,
        least(
            1.0,
            case
                when assigned.alternative_segment_distance is null or assigned.alternative_segment_distance = 0 then 1.0
                else (assigned.alternative_segment_distance - assigned.segment_distance) / assigned.alternative_segment_distance
            end
        )
    ) as segment_confidence,
    assigned.scored_feature_count
from assigned
left join {{ ref('segment_playbook') }} as playbook
    on assigned.segment_id = playbook.segment_id
