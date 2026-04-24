{% snapshot user_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=[
            'department_name',
            'title_name',
            'user_role_id',
            'manager_id',
            'is_active'
        ],
        tags=['dim']
    )
}}

select
    user_id,

    -- SCD tracked attributes
    department_name,
    title_name,
    user_role_id,
    manager_id,
    is_active,

    -- non-SCD attributes
    user_name,
    first_name,
    last_name,
    company_name,
    division_name,
    email,
    phone,
    mobile_phone,
    city,
    state,
    country,
    user_type,
    user_subtype,

    created_date,
    last_modified_date

from {{ ref('int_user') }}

{% endsnapshot %}