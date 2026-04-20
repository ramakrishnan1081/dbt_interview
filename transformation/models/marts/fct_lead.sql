with source as (

    select *
    from {{ ref('int_lead') }}

),

fact as (

    select
        -- ========================
        -- Surrogate Key
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['lead_id']) }} as lead_sk,

        -- ========================
        -- Business Key
        -- ========================
        lead_id,

        -- ========================
        -- Dimension Keys
        -- ========================
        owner_id,

        -- conversion
        converted_account_id,
        converted_contact_id,
        converted_opportunity_id,

        -- ========================
        -- Attributes
        -- ========================
        created_date,
        converted_date,
        last_activity_date,

        -- ========================
        -- Measures
        -- ========================

        case when is_converted then 1 else 0 end as converted_flag,

        case 
            when is_converted and converted_date is not null
            then datediff('day', created_date, converted_date)
        end as conversion_days,

        -- ========================
        -- Lead quality indicators
        -- ========================
        has_email,
        has_phone,
        lead_source,
        status,
        industry,
        rating,

        company,
        title,

        -- ========================
        -- Flags
        -- ========================
        has_opted_out_of_email,
        do_not_call

    from source

)

select * from fact