{% snapshot campaign_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='campaign_id',
        strategy='check',
        check_cols=[
            'campaign_name',
            'type',
            'status',
            'is_active',
            'parent_id',
            'owner_id'
        ],
        tags=['dim']
    )
}}

select
    campaign_id,

    -- SCD tracked attributes
    campaign_name,
    type,
    status,
    is_active,
    parent_id,
    owner_id,

    -- non-SCD attributes
    description,
    start_date,
    end_date,
    last_activity_date,

    created_date,
    last_modified_date

from {{ ref('int_campaign') }}

{% endsnapshot %}