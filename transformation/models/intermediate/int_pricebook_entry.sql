with base as (

    select *
    from {{ ref('stg_salesforce__pricebook_entry') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='pricebook_entry_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        pricebook_entry_id,

        -- ========================
        -- Relationships
        -- ========================
        pricebook2id as pricebook_id,
        product2id as product_id,

        -- ========================
        -- Pricing Info
        -- ========================
        unitprice as unit_price,
        usestandardprice as use_standard_price,

        -- ========================
        -- Status Flags
        -- ========================
        isactive as is_active,
        isarchived as is_archived,

        -- ========================
        -- Audit Fields
        -- ========================
        createddate as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched