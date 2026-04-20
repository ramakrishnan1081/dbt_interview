with source as (

    select *
    from {{ ref('int_accounts') }}

),

-- 1. Select + prepare SCD columns
prepared as (

    select
        account_id,

        -- attributes (SCD2 tracked)
        industry,
        type,
        ownership,
        customer_priority,
        rating,
        active,
        sla,
        owner_id,

        -- non-SCD attributes
        account_name,
        parent_id,
        annual_revenue,
        number_of_employees,
        number_of_locations,
        billing_city,
        billing_state,
        billing_country,
        account_number,
        website,
        phone,
        account_source,
        clean_status,

        -- timestamps
        created_date,
        last_modified_date,

        -- change tracking hash
        md5(
            coalesce(industry,'') ||
            coalesce(type,'') ||
            coalesce(ownership,'') ||
            coalesce(customer_priority,'') ||
            coalesce(rating,'') ||
            coalesce(active,'') ||
            coalesce(sla,'') ||
            coalesce(owner_id,'')
        ) as scd_hash

    from source

),

-- 2. Detect changes over time
scd as (

    select
        *,

        -- start date
        last_modified_date as effective_start_date,

        -- end date using lead
        lead(last_modified_date) over (
            partition by account_id
            order by last_modified_date
        ) as effective_end_date,

        -- detect change vs previous row
        lag(scd_hash) over (
            partition by account_id
            order by last_modified_date
        ) as prev_hash

    from prepared

),

-- 3. Keep only changed records
filtered as (

    select *
    from scd
    where prev_hash is null
       or scd_hash != prev_hash

),

-- 4. Finalize SCD flags + surrogate key
final as (

    select
        -- surrogate key (version-level)
        {{ dbt_utils.generate_surrogate_key(['account_id', 'effective_start_date']) }} as account_sk,

        account_id,

        -- SCD2 attributes
        industry,
        type,
        ownership,
        customer_priority,
        rating,
        active,
        sla,
        owner_id,

        -- non-SCD attributes
        account_name,
        parent_id,
        annual_revenue,
        number_of_employees,
        number_of_locations,
        billing_city,
        billing_state,
        billing_country,
        account_number,
        website,
        phone,
        account_source,
        clean_status,

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