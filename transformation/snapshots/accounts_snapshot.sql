{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='account_id',
        strategy='check',
        check_cols=[
            'billing_street',
            'billing_city',
            'billing_state',
            'billing_postal_code',
            'billing_country',
            'shipping_street',
            'shipping_city',
            'shipping_state',
            'shipping_postal_code',
            'shipping_country', 
            'phone',
            'fax',
            'website',
            'ownership',
            'annual_revenue',
            'number_of_employees',
            'customer_priority',
            'rating',
            'is_active',
            'sla',
            'owner_id',
            'rating'
        ],
        tags=['dim']
    )
}}

select
    account_id,
    account_name,
    account_number,
    industry,
    type,
    parent_id,
    ownership,
    customer_priority,
    rating,
    active__c as is_active,
    sla,
    owner_id,
    annual_revenue,
    ticker_symbol,
    description,
    rating,
    number_of_employees,
    number_of_locations,
    billing_street,
    billing_city,
    billing_state,
    billing_country,
    billing_postal_code,
    shipping_street,
    shipping_city,
    shipping_state,
    shipping_postal_code,
    shipping_country,
    website,
    phone,
    fax,
    sic,
    account_source,
    clean_status,
    created_date,
    upsell_opportunity,
    last_modified_date,
    last_activity_date,

from {{ ref('int_accounts') }}

{% endsnapshot %}