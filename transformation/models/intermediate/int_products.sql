with base as (

    select *
    from {{ ref('stg_salesforce__product_2') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='product_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (

    select
        product_id,

        -- ========================
        -- Core Info
        -- ========================
        name as product_name,
        productcode as product_code,
        description,
        family,
        type,
        productclass as product_class,

        -- ========================
        -- Status Flags
        -- ========================
        isactive as is_active,
        isarchived as is_archived,

        -- ========================
        -- Identifiers
        -- ========================
        stockkeepingunit as sku,
        externalid as external_id,
        externaldatasourceid as external_data_source_id,
        sourceproductid as source_product_id,
        sellerid as seller_id,

        -- ========================
        -- Additional Info
        -- ========================
        displayurl as display_url,
        quantityunitofmeasure as quantity_unit_of_measure,

        -- ========================
        -- Audit Fields
        -- ========================
        createddate as created_date,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp

    from deduplicated

)

select * from enriched