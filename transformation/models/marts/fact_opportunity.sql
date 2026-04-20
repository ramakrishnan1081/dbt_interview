with source as (

    select *
    from {{ ref('int_campaign') }}

),

fact as (

    select
        -- ========================
        -- Surrogate Key
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_sk,

        -- ========================
        -- Business Key
        -- ========================
        campaign_id,

        -- ========================
        -- Dimension Keys
        -- ========================
        owner_id,
        parent_id,

        -- ========================
        -- Attributes
        -- ========================
        start_date,
        end_date,
        created_date,
        last_modified_date,

        -- ========================
        -- Measures
        -- ========================
        expected_revenue,
        budgeted_cost,
        actual_cost,

        number_sent,
        number_of_leads,
        number_of_converted_leads,
        number_of_contacts,
        number_of_responses,
        number_of_opportunities,
        number_of_won_opportunities,

        amount_all_opportunities,
        amount_won_opportunities,

        case 
            when number_sent > 0 
            then number_of_responses * 1.0 / number_sent 
        end as response_rate,

        case 
            when number_of_leads > 0 
            then number_of_converted_leads * 1.0 / number_of_leads 
        end as lead_conversion_rate,

        case 
            when actual_cost > 0 
            then amount_won_opportunities / actual_cost 
        end as roi,

        -- ========================
        -- Flags
        -- ========================
        is_active

    from source

)

select * from fact