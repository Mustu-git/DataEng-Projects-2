{{
    config(
        materialized='table'
    )
}}

with freshness as (

    select
        'stg_taxi_trips'                                    as table_name,
        max(pickup_ts)                                      as last_record_ts,
        current_timestamp                                   as checked_at,
        extract(epoch from (current_timestamp - max(pickup_ts))) / 3600
                                                            as hours_since_last_record,
        case
            when extract(epoch from (current_timestamp - max(pickup_ts))) / 3600 > 25
            then 'STALE'
            else 'FRESH'
        end                                                 as freshness_status
    from {{ source('staging', 'stg_taxi_trips') }}

    union all

    select
        'gold_daily_trips'                                  as table_name,
        max(trip_date)::timestamp                           as last_record_ts,
        current_timestamp                                   as checked_at,
        extract(epoch from (current_timestamp - max(trip_date)::timestamp)) / 3600
                                                            as hours_since_last_record,
        case
            when extract(epoch from (current_timestamp - max(trip_date)::timestamp)) / 3600 > 25
            then 'STALE'
            else 'FRESH'
        end                                                 as freshness_status
    from {{ source('staging', 'gold_daily_trips') }}

)

select * from freshness
