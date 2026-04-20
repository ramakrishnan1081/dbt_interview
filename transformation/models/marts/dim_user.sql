with source as (

    select *
    from {{ ref('int_user') }}

),

-- 1. Prepare + hash
prepared as (

    select
        user_id,

        -- SCD attributes
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
        last_modified_date,

        -- change tracking
        md5(
            coalesce(department_name,'') ||
            coalesce(title_name,'') ||
            coalesce(user_role_id,'') ||
            coalesce(manager_id,'') ||
            coalesce(is_active::text,'')
        ) as scd_hash

    from source

),

-- 2. Detect changes
scd as (

    select
        *,

        last_modified_date as effective_start_date,

        lead(last_modified_date) over (
            partition by user_id
            order by last_modified_date
        ) as effective_end_date,

        lag(scd_hash) over (
            partition by user_id
            order by last_modified_date
        ) as prev_hash

    from prepared

),

-- 3. Keep only changes
filtered as (

    select *
    from scd
    where prev_hash is null
       or scd_hash != prev_hash

),

-- 4. Final
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['user_id', 'effective_start_date']) }} as user_sk,

        user_id,

        -- SCD attributes
        department_name,
        title_name,
        user_role_id,
        manager_id,
        is_active,

        -- non-SCD
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

        effective_start_date,
        coalesce(effective_end_date, '9999-12-31') as effective_end_date,

        case 
            when effective_end_date is null then true 
            else false 
        end as is_current

    from filtered

)

select * from final