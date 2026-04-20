with source as (

    select * 
    from {{ ref('int_user_role') }}

),

renamed as (

    select
        user_role_id,
        name,
        parent_role_id,
        rollup_description, 
        opportunity_access_for_accountowner,
        case_access_for_accountowner,
        contact_access_for_accountowner,
        forecast_user_id,
        may_forecast_manager_share,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        portal_account_id,
        portal_type,
        portal_role,
        portal_account_owner_id
        from source
)

select * from renamed