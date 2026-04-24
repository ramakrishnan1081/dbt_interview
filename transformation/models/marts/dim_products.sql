{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_sk,

    product_id,

    product_name,
    product_code,
    family,
    type,
    product_class,
    is_active,
    is_archived,

    description,
    sku,
    external_id,
    external_data_source_id,
    source_product_id,
    seller_id,
    display_url,
    quantity_unit_of_measure,

    created_date,

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,

    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current

from {{ ref('product_snapshot') }}
where dbt_valid_to is null