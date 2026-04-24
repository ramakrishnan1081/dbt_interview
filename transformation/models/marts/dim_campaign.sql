{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'dbt_valid_from']) }} as campaign_sk,

    campaign_id,

    campaign_name,
    type,
    status,
    is_active,
    parent_id,
    owner_id,

    description,
    start_date,
    end_date,
    last_activity_date,

    created_date,

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date

from {{ ref('campaign_snapshot') }}
where dbt_valid_to is null