-- One row per telemetry reading, with nested fields flattened into columns.
--
-- telemetry_source() decodes the binary protobuf files written by the
-- protobuf consumer (see macros/telemetry_source.sql).
--
-- The consumers deliver messages at-least-once, so the same message can end
-- up in the files more than once. We keep one row per message_id.
--
-- Protobuf enums arrive as their names (CAR, HARSH_BRAKING); they are
-- lower-cased here. New fields added to the .proto file are available in
-- telemetry_source() automatically; add them below to use them in the models.

with source as (

    select * from {{ telemetry_source() }}

),

deduplicated as (

    select
        message_id,
        vehicle_id,
        vin,
        lower(vehicle_type) as vehicle_type,
        {{ adapter.quote('timestamp') }} as recorded_at,
        to_date({{ adapter.quote('timestamp') }}) as event_date,

        source.position.lat as latitude,
        source.position.lon as longitude,
        heading_deg,
        speed_kmh,
        rpm,
        gear,
        engine_temp_c,
        fuel_level_pct,
        battery_voltage,
        odometer_km,

        source.tire_pressure_kpa.front_left as tire_front_left_kpa,
        source.tire_pressure_kpa.front_right as tire_front_right_kpa,
        source.tire_pressure_kpa.rear_left as tire_rear_left_kpa,
        source.tire_pressure_kpa.rear_right as tire_rear_right_kpa,

        check_engine_light,
        dtc_codes,
        transform(events, e -> lower(e)) as events,

        kafka_partition,
        kafka_offset,
        ingested_at

    from source

    qualify row_number() over (partition by message_id order by ingested_at) = 1

),

with_previous as (

    select
        *,
        lag(odometer_km) over (partition by vehicle_id order by recorded_at) as previous_odometer_km

    from deduplicated

)

select
    *,
    -- Distance driven since the vehicle's previous reading. The odometer jumps
    -- when the producer restarts, so implausible jumps count as 0.
    case
        when odometer_km - previous_odometer_km between 0 and 1
            then odometer_km - previous_odometer_km
        else 0
    end as distance_km

from with_previous
