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
        {{ dbt_utils.generate_surrogate_key(['opportunity_history_id']) }} as opportunity_history_sk,

        -- Business Keys
        opportunity_history_id,
        opportunity_id,

        -- Dimension Keys
        created_by_id,

        -- Event Timestamp
        created_date as event_timestamp,

        -- Event Attributes
        stage_name,
        from_opportunity_stage_name,
        forecast_category,
        from_forecast_category,

        -- Measures
        amount,
        expected_revenue,
        probability,
        prev_amount,
        prev_close_date,
        close_date,

        -- Change indicators
        case 
            when prev_amount is not null 
            then amount - prev_amount 
        end as amount_change,

        case 
            when from_opportunity_stage_name != stage_name 
            then 1 else 0 
        end as stage_change_flag

    from source
)

select * from fact
