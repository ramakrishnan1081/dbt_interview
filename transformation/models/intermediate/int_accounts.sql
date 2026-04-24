with source as (

    select * 
    from {{ ref('stg_salesforce__account') }}

),

renamed as (

    select
        account_id,
        isdeleted as is_deleted,
        masterrecordid as master_record_id,
        replace(name,',','|') as account_name,
        type,
        parentid as parent_id,
        replace(billingstreet,',','|') as billing_street,
        replace(billingcity,',','|') as billing_city,
        replace(billingstate,',','|') as billing_state,
        billingpostalcode as billing_postal_code,
        replace(billingcountry,',','|') as billing_country,
        billinglatitude as billing_latitude,
        billinglongitude as billing_longitude,
        billinggeocodeaccuracy as billing_geocode_accuracy,
        replace(shippingstreet,',','|') as shipping_street,
        replace(shippingcity,',','|') as shipping_city,
        replace(shippingstate,',','|') as shipping_state,
        shippingpostalcode as shipping_postal_code,
        replace(shippingcountry,',','|') as shipping_country,
        shippinglatitude as shipping_latitude,
        shippinglongitude as shipping_longitude,
        shippinggeocodeaccuracy as shipping_geocode_accuracy,
        phone,
        fax,
        accountnumber as account_number,
        website,
        sic,
        industry,
        annualrevenue as annual_revenue,
        numberofemployees as number_of_employees,
        ownership,
        tickersymbol as ticker_symbol,
        description,
        rating,
        site,
        ownerid as owner_id,
        createddate as created_date,
        createdbyid as created_by_id,
        lastmodifieddate as last_modified_date,
        lastmodifiedbyid as last_modified_by_id,
        systemmodstamp as system_modstamp,
        lastactivitydate as last_activity_date,
        jigsaw,
        jigsawcompanyid as jigsaw_company_id,
        cleanstatus as clean_status,
        accountsource as account_source,
        dunsnumber as duns_number,
        tradestyle as trade_style,
        naicscode as naics_code,
        naicsdesc as naics_desc,
        yearstarted as year_started,
        sicdesc as sic_desc,
        dandbcompanyid as dandb_company_id,
        operatinghoursid as operating_hours_id,
        customerpriority__c as customer_priority,
        sla__c as sla,
        active__c as active__C,
        numberoflocations__c as number_of_locations,
        upsellopportunity__c as upsell_opportunity,
        slaserialnumber__c as sla_serial_number,
        slaexpirationdate__c as sla_expiration_date

    from source
    where {{ filter_not_deleted('isdeleted') }}
    and {{ filter_isactive('active__c') }}

),

final as (

    {{ dedupe_latest(
        model='renamed',
        partition_by='account_id',
        order_by='last_modified_date'
    ) }}

)

select * from final