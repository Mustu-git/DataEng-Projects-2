{{
    config(
        materialized='incremental',
        unique_key=['table_schema', 'table_name', 'column_name', 'snapshot_date']
    )
}}

/*
  Daily snapshot of column-level schema for monitored tables.
  Run this every day — if a column disappears or changes type,
  you'll see it by comparing today's snapshot to yesterday's.
*/

with today_schema as (

    select
        table_schema,
        table_name,
        column_name,
        data_type,
        is_nullable,
        ordinal_position,
        current_date        as snapshot_date,
        current_timestamp   as snapshot_at
    from information_schema.columns
    where table_schema = 'staging'
    and table_name in (
        'stg_taxi_trips',
        'gold_daily_trips',
        'gold_zone_demand',
        'gold_peak_hours',
        'gold_anomaly_trips'
    )

)

{% if is_incremental() %}

,

previous_schema as (
    select * from {{ this }}
    where snapshot_date = current_date - 1
),

with_drift as (

    select
        c.*,
        case
            when p.column_name is null              then 'COLUMN_ADDED'
            when p.data_type != c.data_type         then 'TYPE_CHANGED'
            when p.is_nullable != c.is_nullable     then 'NULLABILITY_CHANGED'
            else 'UNCHANGED'
        end                 as schema_change
    from today_schema c
    left join previous_schema p
        on p.table_schema = c.table_schema
        and p.table_name  = c.table_name
        and p.column_name = c.column_name

)

select * from with_drift
where snapshot_date = current_date

{% else %}

select *, 'BASELINE' as schema_change
from today_schema

{% endif %}
