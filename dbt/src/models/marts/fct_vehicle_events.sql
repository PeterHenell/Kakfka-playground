-- One row per event (harsh_braking, engine_overheating, ...), with where and
-- when it happened. A reading can carry several events.

with exploded as (

    select
        message_id,
        vehicle_id,
        recorded_at,
        event_date,
        latitude,
        longitude,
        speed_kmh,
        engine_temp_c,
        {{ explode('events') }} as event_type

    from {{ ref('stg_vehicle_telemetry') }}

)

select
    {{ dbt.concat(['message_id', "'-'", 'event_type']) }} as event_id,
    *

from exploded
