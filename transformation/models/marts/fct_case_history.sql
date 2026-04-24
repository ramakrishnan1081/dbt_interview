{{
    config(
      materialized='incremental',
      unique_key='case_history_id',
      tags=['fact']
    )
}}

with max_existing as (
    select max(event_timestamp) as max_event_timestamp
    from {{ this }}
),

source as (
    select *
    from {{ ref('int_case_history') }}
    {% if is_incremental() %}
      where last_modified_date > (select max_event_timestamp from max_existing)
    {% endif %}
),

fact as (

    select
        {{ dbt_utils.generate_surrogate_key(['c.case_history_id']) }} as case_history_sk,
        c.case_history_id,
        c.case_id,
        c.owner_id,
        c.last_modified_by_id,
        c.last_modified_date as event_timestamp,
        c.status,
        c.previous_update,
        1 as event_count
    from source c
    inner join {{ ref('fct_case') }} a
        on c.case_id = a.case_id
    inner join {{ ref('dim_user') }} u
        on c.owner_id = u.user_id

)

select * from fact