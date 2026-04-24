{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['solution_id', 'dbt_valid_from']) }} as solution_sk,

    solution_id,

    solution_name,
    status,
    is_published,
    is_published_in_public_kb,
    is_reviewed,
    owner_id,

    solution_number,
    solution_note,
    is_html,
    times_used,
    case_id,

    created_date,

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,

    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current

from {{ ref('solution_snapshot') }}
where dbt_valid_to is null