with source as (

    select * 
    from {{ ref('stg_salesforce__user') }}

),

renamed as (

   select
        user_id,
        username as user_name,
        firstname as first_name,
        lastname as last_name,
        companyname as company_name,
        division as division_name,
        department as department_name,
        title as title_name,
        street,
        city,
        state,
        postalcode as postal_code,
        country,
        latitude,
        longitude,
        geocodeaccuracy as geocode_accuracy,
        email,
        senderemail as sender_email,
        sendername as sender_name,
        signature,
        stayintouchsubject as stay_intouch_subject,
        stayintouchsignature as stay_intouch_signature,
        stayintouchnote as stay_intouch_note,
        phone,
        fax,
        mobilephone as mobile_phone,
        alias,
        communitynickname as community_nickname,
        isactive as is_active,
        issystemcontrolled as is_system_controlled,
        timezonesidkey as timezone_sid_key,
        userroleid as user_role_id,
        localesidkey as locale_sid_key,
        receivesinfoemails as receives_info_emails,
        receivesadmininfoemails as receives_admin_info_emails,
        emailencodingkey as email_encoding_key,
        profileid as profile_id,
        usertype as user_type,
        usersubtype as user_subtype,
        startday as start_day,
        endday as end_day,
        languagelocalekey as language_locale_key,
        employeenumber as employee_number,
        delegatedapproverid as delegated_approver_id,
        managerid as manager_id,
        lastlogindate as last_login_date,
        lastpasswordchangedate as last_password_change_date,
        createddate as created_date,
        createdbyid as created_by_id,
        lastmodifieddate as last_modified_date,
        lastmodifiedbyid as last_modified_by_id,
        systemmodstamp as system_modstamp,
        numberoffailedlogins as number_of_failed_logins,
        suaccessexpirationdate as su_access_expiration_date,
        suorgadminexpirationdate as su_org_admin_expiration_date,
        offlinetrialexpirationdate as offline_trial_expiration_date,
        wirelesstrialexpirationdate as wireless_trial_expiration_date,
        offlinepdatrialexpirationdate as offline_pda_trial_expiration_date,
        forecastenabled as forecast_enabled,
        contactid as contact_id,
        accountid as account_id,
        callcenterid as call_center_id,
        extension as extension,
        federationidentifier as federation_identifier,
        aboutme as about_me,
        loginlimit as login_limit,
        profilephotoid as profile_photo_id,
        digestfrequency as digest_frequency,
        defaultgroupnotificationfrequency as default_group_notification_frequency,
        jigsawimportlimitoverride as jigsaw_import_limit_override,
        workspaceid as workspace_id,
        sharingtype as sharing_type,
        chatteradoptionstage as chatter_adoption_stage,
        chatteradoptionstagemodifieddate as chatter_adoption_stage_modified_date,
        bannerphotoid as banner_photo_id,
        isprofilephotoactive as is_profile_photo_active,
        individualid as individual_id,
        globalidentity as global_identity

    from source
    where isactive = 1

),

final as (

    {{ dedupe_latest(
        model='renamed',
        partition_by='user_id',
        order_by='system_modstamp'
    ) }}

)

select * from final