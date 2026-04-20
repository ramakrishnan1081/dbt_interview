with source as (

    select *
    from {{ ref('int_case_history') }}

),

fact as (

    select
        -- ========================
        -- Surrogate Key
        -- ========================
        {{ dbt_utils.generate_surrogate_key(['case_history_id']) }} as case_history_sk,

        -- ========================
        -- Business Keys
        -- ========================
        case_history_id,
        case_id,

        -- ========================
        -- Dimension Keys
        -- ========================
        owner_id,
        last_modified_by_id,

        -- ========================
        -- Event Timestamp
        -- ========================
        last_modified_date as event_timestamp,

        -- ========================
        -- Event Attributes
        -- ========================
        status,
        previous_update,

        -- ========================
        -- Derived Metrics
        -- ========================
        1 as event_count

    from source

)

select * from fact