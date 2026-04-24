{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_valid_from']) }} as user_sk,

    user_id,

    department_name,
    title_name,
    user_role_id,
    manager_id,
    is_active,

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

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,

    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current

from {{ ref('user_snapshot') }}
where dbt_valid_to is null