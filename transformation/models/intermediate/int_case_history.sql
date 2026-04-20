with source as (

    select * 
    from {{ ref('stg_salesforce__case_history_2') }}

),

renamed as (

    select
        case_history_id,
        caseid as case_id,
        ownerid as owner_id,
        status,
        previousupdate as previous_update,
        lastmodifieddate as last_modified_date,
        lastmodifiedbyid as last_modified_by_id,
        isdeleted as is_deleted,
        systemmodstamp as system_modstamp

    from source

)

select * from renamed