with base as (

    select *
    from {{ ref('stg_salesforce__lead') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='lead_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        lead_id,

        -- ========================
        -- Core Info
        -- ========================
        firstname as first_name,
        lastname as last_name,
        concat(firstname, ' ', lastname) as full_name,

        company,
        title,
        email,
        phone,
        mobilephone as mobile_phone,
        website,

        -- ========================
        -- Lead Details
        -- ========================
        leadsource as lead_source,
        status,
        industry,
        rating,

        annualrevenue as annual_revenue,
        numberofemployees as number_of_employees,

        -- ========================
        -- Conversion Info
        -- ========================
        isconverted as is_converted,
        cast(converteddate as date) as converted_date,
        convertedaccountid as converted_account_id,
        convertedcontactid as converted_contact_id,
        convertedopportunityid as converted_opportunity_id,

        -- ========================
        -- Flags
        -- ========================
        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,
        donotcall as do_not_call,

        case when email is not null then true else false end as has_email,
        case when phone is not null then true else false end as has_phone,

        -- ========================
        -- Activity
        -- ========================
        lastactivitydate as last_activity_date,

        -- ========================
        -- Ownership
        -- ========================
        ownerid as owner_id,

        -- ========================
        -- Timestamps
        -- ========================
        cast(createddate as date) as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched