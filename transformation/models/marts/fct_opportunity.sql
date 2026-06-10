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
        c.opportunity_id,
        a.account_sk,
        ct.contact_sk,
        cp.campaign_sk,
        u.user_sk,
        c.created_date,
        c.close_date,
        c.last_stage_change_date,
        c.amount,
        c.probability,
        c.expected_revenue,
        c.total_opportunity_quantity,
        case when c.is_won then 1 else 0 end as won_flag,
        case when c.is_closed then 1 else 0 end as closed_flag,
        case 
            when c.is_won and c.close_date is not null and c.created_date is not null
            then date_diff('day', c.created_date, c.close_date)
        end as sales_cycle_days,
        c.stage_name,
        c.stage_sort_order,
        c.forecast_category,
        c.forecast_category_name,
        c.type,
        c.lead_source,
        c.next_step,
        c.has_opportunity_line_item,
        c.is_private,
        c.delivery_installation_status,
        c.tracking_number,
        c.order_number,
        c.current_generators,
        c.main_competitors,
        c.event_timestamp
    from source c
    inner join {{ ref('dim_accounts') }} a on c.account_id = a.account_id
    and a.effective_end_date = '9999-12-31'
    inner join {{ ref('dim_contacts') }} ct on c.contact_id = ct.contact_id
    and ct.effective_end_date = '9999-12-31'
    inner join {{ ref('dim_campaign') }} cp on c.campaign_id = cp.campaign_id
    and cp.effective_end_date = '9999-12-31'
    inner join {{ ref('dim_user') }} u on c.owner_id = u.user_id
    and u.effective_end_date = '9999-12-31'
)

select * from fact