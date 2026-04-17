with base as (

    select *
    from {{ ref('stg_salesforce__campaign') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='campaign_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        campaign_id,

        -- ========================
        -- Core Info
        -- ========================
        name as campaign_name,
        type,
        status,
        isactive as is_active,
        description,

        -- ========================
        -- Hierarchy
        -- ========================
        parentid as parent_id,

        -- ========================
        -- Financials
        -- ========================
        expectedrevenue as expected_revenue,
        budgetedcost as budgeted_cost,
        actualcost as actual_cost,

        -- ========================
        -- Performance Metrics
        -- ========================
        numbersent as number_sent,
        numberofleads as number_of_leads,
        numberofconvertedleads as number_of_converted_leads,
        numberofcontacts as number_of_contacts,
        numberofresponses as number_of_responses,
        numberofopportunities as number_of_opportunities,
        numberofwonopportunities as number_of_won_opportunities,

        amountallopportunities as amount_all_opportunities,
        amountwonopportunities as amount_won_opportunities,

        -- ========================
        -- Dates
        -- ========================
        startdate as start_date,
        enddate as end_date,
        lastactivitydate as last_activity_date,

        -- ========================
        -- Ownership
        -- ========================
        ownerid as owner_id,

        -- ========================
        -- Audit Fields
        -- ========================
        createddate as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched