{{
    config(
      materialized='incremental',
      unique_key='case_history_id'
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
        {{ dbt_utils.generate_surrogate_key(['case_history_id']) }} as case_history_sk,
        case_history_id,
        case_id,
        owner_id,
        last_modified_by_id,
        last_modified_date as event_timestamp,
        status,
        previous_update,
        1 as event_count
    from source

)

select * from fact