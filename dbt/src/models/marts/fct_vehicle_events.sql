-- One row per event (harsh_braking, engine_overheating, ...), with where and
-- when it happened. A reading can carry several events.
--
-- Incremental: events never change once they happened, so each run only adds
-- the events from Kafka records it hasn't seen yet. Within a Kafka partition
-- the offsets only grow, so "new" means: an offset higher than the highest
-- one already in the table, for the same partition.
--
-- Delivery is at-least-once, so a run can still see an event again. The merge
-- on unique_key updates it instead of adding a second row. Rebuild the whole
-- table with `dbt build --full-refresh`.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

with readings as (

    select readings.*

    from {{ ref('stg_vehicle_telemetry') }} as readings

    {% if is_incremental() %}
    -- {{ this }} is this model's own table, as it was after the last run.
    left join (
        select kafka_partition, max(kafka_offset) as max_kafka_offset
        from {{ this }}
        group by kafka_partition
    ) as loaded
        on readings.kafka_partition = loaded.kafka_partition

    where loaded.max_kafka_offset is null  -- a partition we haven't seen yet
       or readings.kafka_offset > loaded.max_kafka_offset
    {% endif %}

),

exploded as (

    select
        message_id,
        vehicle_id,
        recorded_at,
        event_date,
        latitude,
        longitude,
        speed_kmh,
        engine_temp_c,
        kafka_partition,
        kafka_offset,
        explode(events) as event_type

    from readings

)

select
    {{ dbt.concat(['message_id', "'-'", 'event_type']) }} as event_id,
    *

from exploded
