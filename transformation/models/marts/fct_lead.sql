{{
    config(
      materialized='incremental',
      unique_key='lead_id',
      tags=['fact']
    )
}}

with max_existing as (
    -- Use the same column name that exists in the final model
    select max(system_modstamp) as max_system_modstamp
    from {{ this }}
),

source as (
    select *
    from {{ ref('int_lead') }}
    {% if is_incremental() %}
      where system_modstamp > (select max_system_modstamp from max_existing)
    {% endif %}
),

fact as (
    select
        {{ dbt_utils.generate_surrogate_key(['lead_id']) }} as lead_sk,
        lead_id,
        owner_id,
        converted_account_id,
        converted_contact_id,
        converted_opportunity_id,
        created_date,
        converted_date,
        last_activity_date,
        case when is_converted then 1 else 0 end as converted_flag,
        case when is_converted and converted_date is not null
             then datediff('day', created_date, converted_date)
        end as conversion_days,
        has_email,
        has_phone,
        lead_source,
        status,
        industry,
        rating,
        company,
        title,
        has_opted_out_of_email,
        do_not_call,
        -- expose system_modstamp so it exists in the target table
        system_modstamp
    from source
)

select * from fact
