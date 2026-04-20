with source as (

    select * 
    from {{ ref('stg_salesforce__opportunity_history') }}

),

renamed as (

   select
        opportunity_history_id,
        opportunityid as opportunity_id,
        createdbyid as created_by_id,
        createddate as created_date,
        createddateforinsert as created_date_for_insert,
        stagename as stage_name,
        cast(amount as float) as amount,
        cast(expectedrevenue as float) as expected_revenue,
        cast(closedate as date) as close_date,
        probability as probability,
        fromforecastcategory as from_forecast_category,
        forecastcategory as forecast_category,
        prevforecastupdate as prev_forecast_update,
        fromopportunitystagename as from_opportunity_stage_name,
        prevopportunitystageupdate as prev_opportunity_stage_update,
        cast(validthroughdate as date) as validity_through_date,
        systemmodstamp as system_modstamp,
        isdeleted as is_deleted,
        cast(prevamount as float) as prev_amount,
        cast(prevclosedate as date) as prev_close_date

    from source
    where {{ filter_not_deleted('isdeleted') }}

)

select * from renamed