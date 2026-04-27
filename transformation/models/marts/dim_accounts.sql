{{ config(tags=['dim']) }}

select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'ticker_symbol']) }} as account_sk,

    account_id,
    account_name,
    account_number,
    industry,
    type,
    ownership,
    description,
    customer_priority,
    rating,
    is_active,
    sla,
    owner_id,
    parent_id,
    annual_revenue,
    number_of_employees,
    number_of_locations,
    billing_city,
    billing_state,
    billing_country,
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
    ticker_symbol,
    created_date,
    upsell_opportunity,
    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date

from {{ ref('accounts_snapshot') }}
where dbt_valid_to is null