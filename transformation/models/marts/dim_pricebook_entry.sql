{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['pricebook_entry_id', 'dbt_valid_from']) }} as pricebook_entry_sk,

    pricebook_entry_id,

    pricebook_id,
    product_id,

    unit_price,
    use_standard_price,
    is_active,
    is_archived,

    created_date,

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,

    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current

from {{ ref('pricebook_entry_snapshot') }}
where dbt_valid_to is null