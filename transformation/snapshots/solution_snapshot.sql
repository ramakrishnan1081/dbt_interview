{% snapshot solution_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='solution_id',
        strategy='check',
        check_cols=[
            'solution_name',
            'status',
            'is_published',
            'is_published_in_public_kb',
            'is_reviewed',
            'owner_id'
        ],
        tags=['dim']
    )
}}

select
    solution_id,

    -- SCD tracked attributes
    solution_name,
    status,
    is_published,
    is_published_in_public_kb,
    is_reviewed,
    owner_id,

    -- non-SCD attributes
    solution_number,
    solution_note,
    is_html,
    times_used,
    case_id,

    created_date,
    last_modified_date

from {{ ref('int_solution') }}

{% endsnapshot %}