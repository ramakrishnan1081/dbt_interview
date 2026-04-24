{{
    config(
        materialized='incremental',
        unique_key='opportunity_history_id',
        tags=['fact']
    )
}}

with source as (

    select *
    from {{ ref('int_opportunity_history') }}
    {% if is_incremental() %}
      -- Only pull rows newer than the latest event_timestamp already in the target table
      where system_modstamp > (select max(event_timestamp) from {{ this }})
    {% endif %}

),

fact as (

    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.opportunity_history_id']) }} as opportunity_history_sk,

        -- Business Keys
        c.opportunity_history_id,
        c.opportunity_id,

        -- Dimension Keys
        c.created_by_id,

        -- Event Timestamp
        c.created_date as event_timestamp,

        -- Event Attributes
        c.stage_name,
        c.from_opportunity_stage_name,
        c.forecast_category,
        c.from_forecast_category,

        -- Measures
        c.amount,
        c.expected_revenue,
        c.probability,
        c.prev_amount,
        c.prev_close_date,
        c.close_date,

        -- Change indicators
        case 
            when c.prev_amount is not null 
            then c.amount - c.prev_amount 
        end as amount_change,

        case 
            when c.from_opportunity_stage_name != c.stage_name 
            then 1 else 0 
        end as stage_change_flag

    from source c 
    inner join {{ ref('fct_opportunity') }} o on c.opportunity_id = o.opportunity_id
)

select * from fact
