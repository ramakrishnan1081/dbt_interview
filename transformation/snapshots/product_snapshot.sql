{% snapshot product_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_name',
            'product_code',
            'family',
            'type',
            'product_class',
            'is_active',
            'is_archived'
        ],
        tags=['dim']
    )
}}

select
    product_id,

    -- SCD tracked attributes
    product_name,
    product_code,
    family,
    type,
    product_class,
    is_active,
    is_archived,

    -- non-SCD attributes
    description,
    sku,
    external_id,
    external_data_source_id,
    source_product_id,
    seller_id,
    display_url,
    quantity_unit_of_measure,

    created_date,
    last_modified_date

from {{ ref('int_products') }}

{% endsnapshot %}