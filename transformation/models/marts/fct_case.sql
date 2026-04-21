{{
    config(
      materialized='incremental',
      unique_key='case_id'
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
        {{ dbt_utils.generate_surrogate_key(['case_id']) }} as case_sk,

        -- ========================
        -- Business Keys
        -- ========================
        case_id,
        case_number,

        -- ========================
        -- Dimension Keys
        -- ========================
        account_id,
        contact_id,
        owner_id,
        product_id,
        parent_case_id,

        -- ========================
        -- Attributes
        -- ========================
        created_date,
        closed_date,
        sla_start_date,
        sla_exit_date,
        stop_start_date,
        last_modified_date,

        -- ========================

        -- resolution time
        case 
            when closed_date is not null 
            then datediff('day', created_date, closed_date)
        end as resolution_days,

        -- SLA duration
        case 
            when sla_start_date is not null 
             and sla_exit_date is not null
            then datediff('day', sla_start_date, sla_exit_date)
        end as sla_duration_days,

        -- ========================
        -- Flags
        -- ========================
        is_closed,
        is_escalated,
        is_closed_on_create,
        is_stopped,
        sla_violation,
        potential_liability,

        type,
        status,
        reason,
        priority,
        origin,

        subject

    from source

)

select * from fact