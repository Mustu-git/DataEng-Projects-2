{{
    config(
        materialized='incremental',
        unique_key=['table_name', 'checked_date']
    )
}}

with current_counts as (

    select
        'stg_taxi_trips'            as table_name,
        current_date                as checked_date,
        current_timestamp           as checked_at,
        count(*)                    as row_count
    from {{ source('staging', 'stg_taxi_trips') }}

    union all

    select
        'gold_daily_trips'          as table_name,
        current_date                as checked_date,
        current_timestamp           as checked_at,
        count(*)                    as row_count
    from {{ source('staging', 'gold_daily_trips') }}

    union all

    select
        'gold_zone_demand'          as table_name,
        current_date                as checked_date,
        current_timestamp           as checked_at,
        count(*)                    as row_count
    from {{ source('staging', 'gold_zone_demand') }}

    union all

    select
        'gold_peak_hours'           as table_name,
        current_date                as checked_date,
        current_timestamp           as checked_at,
        count(*)                    as row_count
    from {{ source('staging', 'gold_peak_hours') }}

),

{% if is_incremental() %}

history as (
    select * from {{ this }}
),

with_drift as (

    select
        c.table_name,
        c.checked_date,
        c.checked_at,
        c.row_count,
        avg(h.row_count)            as avg_7day_row_count,
        case
            when avg(h.row_count) is null then 'BASELINE'
            when abs(c.row_count - avg(h.row_count))
                / nullif(avg(h.row_count), 0) > 0.2 then 'DRIFT_DETECTED'
            else 'NORMAL'
        end                         as drift_status
    from current_counts c
    left join history h
        on h.table_name = c.table_name
        and h.checked_date >= current_date - 7
        and h.checked_date < current_date
    group by 1, 2, 3, 4

)

{% else %}

-- First run: no history yet, everything is BASELINE
with_drift as (

    select
        table_name,
        checked_date,
        checked_at,
        row_count,
        null::numeric               as avg_7day_row_count,
        'BASELINE'                  as drift_status
    from current_counts

)

{% endif %}

select * from with_drift
