with source as (

    select *
    from {{ ref('int_contact') }}

),

-- 1. Prepare + hash
prepared as (

    select
        contact_id,

        -- SCD tracked attributes
        account_id,
        owner_id,
        title,
        department,
        level,

        -- non-SCD attributes
        first_name,
        last_name,
        salutation,
        email,
        phone,
        mobile_phone,
        mailing_city,
        mailing_state,
        mailing_country,
        birthdate,
        gender_identity,
        languages,
        lead_source,
        has_opted_out_of_email,
        do_not_call,
        last_activity_date,
        clean_status,

        created_date,
        last_modified_date,

        -- change tracking hash
        md5(
            coalesce(account_id,'') ||
            coalesce(owner_id,'') ||
            coalesce(title,'') ||
            coalesce(department,'') ||
            coalesce(level,'')
        ) as scd_hash

    from source

),

-- 2. Detect changes
scd as (

    select
        *,

        last_modified_date as effective_start_date,

        lead(last_modified_date) over (
            partition by contact_id
            order by last_modified_date
        ) as effective_end_date,

        lag(scd_hash) over (
            partition by contact_id
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

-- 4. Finalize
final as (

    select
        -- surrogate key (versioned)
        {{ dbt_utils.generate_surrogate_key(['contact_id', 'effective_start_date']) }} as contact_sk,

        contact_id,

        -- SCD attributes
        account_id,
        owner_id,
        title,
        department,
        level,

        -- non-SCD attributes
        first_name,
        last_name,
        salutation,
        email,
        phone,
        mobile_phone,
        mailing_city,
        mailing_state,
        mailing_country,
        birthdate,
        gender_identity,
        languages,
        lead_source,
        has_opted_out_of_email,
        do_not_call,
        last_activity_date,
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