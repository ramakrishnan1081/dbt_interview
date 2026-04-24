{{
    config(
      materialized='incremental',
      unique_key='case_id',
      tags=['fact']
    )
}}
with source as (

    select *
    from {{ ref('int_case') }}
    {% if is_incremental() %}
      where last_modified_date > (select max(last_modified_date) from {{ this }})
    {% endif %}

),
fact as (

    select
        -- ========================
        -- Surrogate Key
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['c.case_id']) }} as case_sk,

        -- ========================
        -- Business Keys
        -- ========================
        c.case_id,
        c.case_number,

        -- ========================
        -- Dimension Keys
        -- ========================
        c.account_id,
        c.contact_id,
        c.owner_id,
        c.product_id,
        c.parent_case_id,

        -- ========================
        -- Attributes
        -- ========================
        c.created_date,
        c.closed_date,
        c.sla_start_date,
        c.sla_exit_date,
        c.stop_start_date,
        c.last_modified_date,
        now() as  system_modstamp,

        -- ========================

        -- resolution time
        case 
            when c.closed_date is not null 
            then datediff('day', c.created_date, c.closed_date)
        end as resolution_days,

        -- SLA duration
        case 
            when c.sla_start_date is not null 
             and c.sla_exit_date is not null
            then datediff('day', c.sla_start_date, c.sla_exit_date)
        end as sla_duration_days,

        -- ========================
        -- Flags
        -- ========================
        c.is_closed,
        c.is_escalated,
        c.is_closed_on_create,
        c.is_stopped,
        c.sla_violation,
        c.potential_liability,

        c.type,
        c.status,
        c.reason,
        c.priority,
        c.origin,

        c.subject

    from source c
    inner join {{ ref('dim_accounts') }} a on c.account_id = a.account_id
    inner join {{ ref('dim_contacts') }} ct on c.contact_id = ct.contact_id
    inner join {{ ref('dim_user') }} u on c.owner_id = u.user_id
    left join {{ ref('dim_products') }} p on c.product_id = p.product_id

)

select * from fact