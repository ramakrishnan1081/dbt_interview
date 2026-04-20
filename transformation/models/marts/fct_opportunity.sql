with source as (

    select *
    from {{ ref('int_opportunity') }}

),

fact as (

    select
        -- ========================
        -- Surrogate Key
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }} as opportunity_sk,

        -- ========================
        -- Business Key
        -- ========================
        opportunity_id,

        -- ========================
        -- Dimension Keys
        -- ========================
        account_id,
        contact_id,
        campaign_id,
        owner_id,

        -- ========================
        -- Dates
        -- ========================
        created_date,
        close_date,
        last_stage_change_date,

        -- ========================
        -- Measures
        -- ========================
        amount,
        probability,
        expected_revenue,
        total_opportunity_quantity,

        -- ========================
        -- Derived Metrics
        -- ========================
        case when is_won then 1 else 0 end as won_flag,
        case when is_closed then 1 else 0 end as closed_flag,

        case 
            when is_won and close_date is not null and created_date is not null
            then datediff('day', created_date, close_date)
        end as sales_cycle_days,

        -- ========================
        -- Degenerate Dimensions
        -- ========================
        stage_name,
        stage_sort_order,
        forecast_category,
        forecast_category_name,
        type,
        lead_source,
        next_step,

        -- ========================
        -- Flags
        -- ========================
        has_opportunity_line_item,
        is_private,

        -- ========================
        -- Custom fields
        -- ========================
        delivery_installation_status,
        tracking_number,
        order_number,
        current_generators,
        main_competitors

    from source

)

select * from fact