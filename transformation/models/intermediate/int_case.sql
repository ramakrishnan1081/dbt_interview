with base as (

    select *
    from {{ ref('stg_salesforce__case') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='case_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        case_id,

        -- ========================
        -- Core Info
        -- ========================
        casenumber as case_number,
        subject,
        description,
        type,
        status,
        reason,
        priority,
        origin,

        -- ========================
        -- Relationships
        -- ========================
        contactid as contact_id,
        accountid as account_id,
        ownerid as owner_id,
        parentid as parent_case_id,

        -- ========================
        -- Product / Asset
        -- ========================
        productid as product_id,
        assetid as asset_id,

        -- ========================
        -- SLA & Escalation
        -- ========================
        isclosed as is_closed,
        closeddate as closed_date,
        isescalated as is_escalated,

        slastartdate as sla_start_date,
        slaexitdate as sla_exit_date,
        isstopped as is_stopped,
        stopstartdate as stop_start_date,

        -- ========================
        -- Flags / Indicators
        -- ========================
        isclosedoncreate as is_closed_on_create,
        slaviolation__c as sla_violation,
        potentialliability__c as potential_liability,

        -- ========================
        -- Custom / Business Fields
        -- ========================
        engineeringreqnumber__c as engineering_req_number,
        product__c as product_name,

        -- ========================
        -- Activity
        -- ========================
        eventsprocesseddate as events_processed_date,

        -- ========================
        -- Audit Fields
        -- ========================
        createddate as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched