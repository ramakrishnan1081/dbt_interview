with base as (

    select *
    from {{ ref('stg_salesforce__solution') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='solution_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        solution_id,

        -- ========================
        -- Core Info
        -- ========================
        solutionnumber as solution_number,
        solutionname as solution_name,
        solutionnote as solution_note,
        status,

        -- ========================
        -- Publishing & Review
        -- ========================
        ispublished as is_published,
        ispublishedinpublickb as is_published_in_public_kb,
        isreviewed as is_reviewed,
        ishtml as is_html,

        -- ========================
        -- Usage Metrics
        -- ========================
        timesused as times_used,

        -- ========================
        -- Relationships
        -- ========================
        caseid as case_id,
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