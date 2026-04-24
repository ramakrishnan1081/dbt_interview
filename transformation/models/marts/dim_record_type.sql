{{ config(tags=['dim']) }}

with source as (

    select * 
    from {{ ref('int_record_type') }}

),

renamed as (

    select
        record_type_id,
        name,
        module_name_space,
        description,
        business_process_id,
        s_object_type,
        is_active,
        created_by_id,
        created_date,
        last_modified_by_id,
        last_modified_date,
        system_modstamp,
        is_deleted
    from source

)

select * from renamed