with source as (

    select * 
    from {{ ref('stg_salesforce__record_type') }}

),

renamed as (

    select
        record_type_id,
        name,
        modulenamespace as module_name_space,
        description,
        businessprocessid as business_process_id,
        sobjecttype as s_object_type,
        isactive as is_active,
        createdbyid as created_by_id,
        createddate as created_date,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_date,
        systemmodstamp as system_modstamp,
        isdeleted as is_deleted
    from source
    where {{ filter_not_deleted('isdeleted') }}

)

select * from renamed