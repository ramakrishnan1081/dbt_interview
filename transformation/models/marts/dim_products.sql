with source as (

    select *
    from {{ ref('int_products') }}

),

-- 1. Prepare + hash
prepared as (

    select
        product_id,

        -- SCD attributes
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
        last_modified_date,

        -- change tracking hash
        md5(
            coalesce(product_name,'') ||
            coalesce(product_code,'') ||
            coalesce(family,'') ||
            coalesce(type,'') ||
            coalesce(product_class,'') ||
            coalesce(is_active::text,'') ||
            coalesce(is_archived::text,'')
        ) as scd_hash

    from source

),

-- 2. Detect changes
scd as (

    select
        *,

        last_modified_date as effective_start_date,

        lead(last_modified_date) over (
            partition by product_id
            order by last_modified_date
        ) as effective_end_date,

        lag(scd_hash) over (
            partition by product_id
            order by last_modified_date
        ) as prev_hash

    from prepared

),

-- 3. Keep only changed records
filtered as (

    select *
    from scd
    where prev_hash is null
       or scd_hash != prev_hash

),

-- 4. Finalize
final as (

    select
        -- surrogate key (versioned)
        {{ dbt_utils.generate_surrogate_key(['product_id', 'effective_start_date']) }} as product_sk,

        product_id,

        -- SCD attributes
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

        effective_start_date,
        coalesce(effective_end_date, '9999-12-31') as effective_end_date,

        case 
            when effective_end_date is null then true 
            else false 
        end as is_current

    from filtered

)

select * from final