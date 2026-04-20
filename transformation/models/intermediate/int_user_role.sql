with source as (

    select * 
    from {{ ref('stg_salesforce__user_role') }}

),

renamed as (
    select
        user_role_id,
        name,
        parentroleid as parent_role_id,
        rollupdescription as rollup_description, 
        opportunityaccessforaccountowner as opportunity_access_for_accountowner,
        caseaccessforaccountowner as case_access_for_accountowner,
        contactaccessforaccountowner as contact_access_for_accountowner,
        forecastuserid as forecast_user_id,
        mayforecastmanagershare as may_forecast_manager_share,
        lastmodifieddate as last_modified_date,
        lastmodifiedbyid as last_modified_by_id,
        systemmodstamp as system_modstamp,
        portalaccountid as portal_account_id,
        portaltype as portal_type,
        portalrole as portal_role,
        portalaccountownerid as portal_account_owner_id

        from source

)
select * from renamed