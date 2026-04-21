with base as (

    select *
    from {{ ref('stg_salesforce__opportunity') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='opportunity_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        opportunity_id,

        -- ========================
        -- Core Info
        -- ========================
        name as opportunity_name,
        description,

        -- ========================
        -- Account & Relationships
        -- ========================
        accountid as account_id,
        contactid as contact_id,
        campaignid as campaign_id,
        ownerid as owner_id,

        -- ========================
        -- Pipeline / Stage
        -- ========================
        stagename as stage_name,
        stagesortorder as stage_sort_order,
        forecastcategory as forecast_category,
        forecastcategoryname as forecast_category_name,

        isclosed as is_closed,
        iswon as is_won,

        -- ========================
        -- Financials
        -- ========================
        amount,
        probability,
        expectedrevenue as expected_revenue,
        totalopportunityquantity as total_opportunity_quantity,

        -- ========================
        -- Dates
        -- ========================
        closedate as close_date,
        cast(laststagechangedate as timestamp) as last_stage_change_date,
        cast(lastactivitydate as timestamp) as last_activity_date,

        fiscalyear as fiscal_year,
        fiscalquarter as fiscal_quarter,

        -- ========================
        -- Sales Info
        -- ========================
        type,
        leadsource as lead_source,
        nextstep as next_step,

        -- ========================
        -- Flags / Indicators
        -- ========================
        hasopportunitylineitem as has_opportunity_line_item,
        isprivate as is_private,

        -- ========================
        -- Custom Fields
        -- ========================
        deliveryinstallationstatus__c as delivery_installation_status,
        trackingnumber__c as tracking_number,
        ordernumber__c as order_number,
        currentgenerators__c as current_generators,
        maincompetitors__c as main_competitors,

        -- ========================
        -- Timestamps
        -- ========================
        createddate as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched