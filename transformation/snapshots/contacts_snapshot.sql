{% snapshot contact_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='contact_id',
        strategy='check',
        check_cols=[
            'account_id',
            'owner_id',
            'title',
            'department',
            'level'
        ],
        tags=['dim']
    )
}}

select
    contact_id,

    -- SCD tracked attributes
    account_id,
    owner_id,
    title,
    department,
    level,

    -- non-SCD attributes
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
    last_modified_date

from {{ ref('int_contact') }}

{% endsnapshot %}