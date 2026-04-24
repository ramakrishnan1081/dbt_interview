{{
    config(
      materialized='incremental',
      unique_key='opportunity_id',
      tags=['fact']
    )
}}

with max_existing as (

    {% if is_incremental() %}
        select max(event_timestamp) as max_event_timestamp
        from {{ this }}
    {% else %}
        select null as max_event_timestamp
    {% endif %}

),

source as (
    select *,
           greatest(created_date, close_date, last_stage_change_date) as event_timestamp
    from {{ ref('int_opportunity') }}
    {% if is_incremental() %}
      where greatest(created_date, close_date, last_stage_change_date) 
            > coalesce((select max_event_timestamp from max_existing), '1900-01-01')
    {% endif %}
),

fact as (
    select
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }} as opportunity_sk,
        opportunity_id,
        account_id,
        contact_id,
        campaign_id,
        owner_id,
        created_date,
        close_date,
        last_stage_change_date,
        amount,
        probability,
        expected_revenue,
        total_opportunity_quantity,
        case when is_won then 1 else 0 end as won_flag,
        case when is_closed then 1 else 0 end as closed_flag,
        case 
            when is_won and close_date is not null and created_date is not null
            then date_diff('day', created_date, close_date)
        end as sales_cycle_days,
        stage_name,
        stage_sort_order,
        forecast_category,
        forecast_category_name,
        type,
        lead_source,
        next_step,
        has_opportunity_line_item,
        is_private,
        delivery_installation_status,
        tracking_number,
        order_number,
        current_generators,
        main_competitors,
        event_timestamp
    from source
)

select * from fact