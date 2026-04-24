{% snapshot pricebook_entry_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='pricebook_entry_id',
        strategy='check',
        check_cols=[
            'unit_price',
            'use_standard_price',
            'is_active',
            'is_archived'
        ],
        tags=['dim']
    )
}}

select
    pricebook_entry_id,

    pricebook_id,
    product_id,

    -- SCD tracked attributes
    unit_price,
    use_standard_price,
    is_active,
    is_archived,

    created_date,
    last_modified_date

from {{ ref('int_pricebook_entry') }}

{% endsnapshot %}