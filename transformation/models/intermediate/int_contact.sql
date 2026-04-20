with base as (

    select *
    from {{ ref('stg_salesforce__contact') }}

),

filtered as (

    select *
    from base
    where {{ filter_not_deleted('isdeleted') }}

),

deduplicated as (

    {{ dedupe_latest(
        model='filtered',
        partition_by='contact_id',
        order_by='systemmodstamp'
    ) }}

),

enriched as (
select contact_id,
isdeleted as is_deleted,
        masterrecordid as master_record_id,
        accountid as account_id,
        salutation,
        firstname as first_name,
        lastname as last_name,

        otherstreet as other_street,
        othercity as other_city,
        otherstate as other_state,
        otherpostalcode as other_postal_code,
        othercountry as other_country,
        otherlatitude as other_latitude,
        otherlongitude as other_longitude,
        othergeocodeaccuracy as other_geocode_accuracy,

        mailingstreet as mailing_street,
        mailingcity as mailing_city,
        mailingstate as mailing_state,
        mailingpostalcode as mailing_postal_code,
        mailingcountry as mailing_country,
        mailinglatitude as mailing_latitude,
        mailinglongitude as mailing_longitude,
        mailinggeocodeaccuracy as mailing_geocode_accuracy,

        phone,
        fax,
        mobilephone as mobile_phone,
        homephone as home_phone,
        otherphone as other_phone,
        assistantphone as assistant_phone,

        reportstoid as reports_to_id,
        email,
        title,
        department,
        assistantname as assistant_name,
        leadsource as lead_source,
        birthdate,

        description,
        ownerid as owner_id,

        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,
        donotcall as do_not_call,

        createddate as created_date,
        createdbyid as created_by_id,
        lastmodifieddate as last_modified_date,
        lastmodifiedbyid as last_modified_by_id,
        systemmodstamp as system_modstamp,
        lastactivitydate as last_activity_date,

        lastcurequestdate as last_cu_request_date,
        lastcuupdatedate as last_cu_update_date,

        emailbouncedreason as email_bounced_reason,
        emailbounceddate as email_bounced_date,

        jigsaw,
        jigsawcontactid as jigsaw_contact_id,
        cleanstatus as clean_status,
        individualid as individual_id,

        pronouns,
        genderidentity as gender_identity,

        level__c as level,
        languages__c as languages

    from deduplicated

)

select * from enriched