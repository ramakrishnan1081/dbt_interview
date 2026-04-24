{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['contact_id', 'dbt_valid_from']) }} as contact_sk,

    contact_id,

    account_id,
    owner_id,
    title,
    department,
    level,

    first_name,
    last_name,
    salutation,
    email,
    phone,
    mobile_phone,
    mailing_city,
    mailing_state,
    mailing_country,
    birthdate,
    gender_identity,
    languages,
    lead_source,
    has_opted_out_of_email,
    do_not_call,
    last_activity_date,
    clean_status,

    created_date,

    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,

    case 
        when dbt_valid_to is null then true 
        else false 
    end as is_current

from {{ ref('contact_snapshot') }}
where dbt_valid_to is null