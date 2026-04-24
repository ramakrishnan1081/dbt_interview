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
        {{ dbt_utils.generate_surrogate_key(['c.lead_id']) }} as lead_sk,
        c.lead_id,
        c.owner_id,
        c.converted_account_id,
        c.converted_contact_id,
        c.converted_opportunity_id,
        c.created_date,
        c.converted_date,
        c.last_activity_date,
        case when c.is_converted then 1 else 0 end as converted_flag,
        case when c.is_converted and c.converted_date is not null
             then datediff('day', c.created_date, c.converted_date)
        end as conversion_days,
        c.has_email,
        c.has_phone,
        c.lead_source,
        c.status,
        c.industry,
        c.rating,
        c.company,
        c.title,
        c.has_opted_out_of_email,
        c.do_not_call,
        -- expose system_modstamp so it exists in the target table
        c.system_modstamp
    from source c
    inner join {{ ref('dim_accounts') }} a
        on c.converted_account_id = a.account_id
    inner join {{ ref('dim_contacts') }} ct
        on c.converted_contact_id = ct.contact_id
    inner join {{ ref('fct_opportunity') }} o
        on c.converted_opportunity_id = o.opportunity_id
)

select * from fact
