with source as (

    select *
    from {{ ref('int_campaign') }}

),

-- 1. Prepare + hash
prepared as (

    select
        campaign_id,

        -- SCD attributes
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
        last_modified_date,

        -- change tracking
        md5(
            coalesce(campaign_name,'') ||
            coalesce(type,'') ||
            coalesce(status,'') ||
            coalesce(is_active::text,'') ||
            coalesce(parent_id,'') ||
            coalesce(owner_id,'')
        ) as scd_hash

    from source

),

-- 2. Detect changes
scd as (

    select
        *,

        last_modified_date as effective_start_date,

        lead(last_modified_date) over (
            partition by campaign_id
            order by last_modified_date
        ) as effective_end_date,

        lag(scd_hash) over (
            partition by campaign_id
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
        {{ dbt_utils.generate_surrogate_key(['campaign_id', 'effective_start_date']) }} as campaign_sk,

        campaign_id,

        -- SCD attributes
        campaign_name,
        type,
        status,
        is_active,
        parent_id,
        owner_id,

        -- non-SCD
        description,
        start_date,
        end_date,
        last_activity_date,

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