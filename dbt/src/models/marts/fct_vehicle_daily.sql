-- One row per vehicle per day: how far it drove and how it was doing.

select
    vehicle_id,
    event_date,
    count(*) as reading_count,
    min(recorded_at) as first_reading_at,
    max(recorded_at) as last_reading_at,

    sum(distance_km) as distance_km,
    avg(speed_kmh) as avg_speed_kmh,
    max(speed_kmh) as max_speed_kmh,
    avg(case when speed_kmh > 1 then speed_kmh end) as avg_moving_speed_kmh,

    avg(engine_temp_c) as avg_engine_temp_c,
    max(engine_temp_c) as max_engine_temp_c,
    min(fuel_level_pct) as min_fuel_level_pct,
    min(battery_voltage) as min_battery_voltage,
    min(least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa)) as min_tire_pressure_kpa,

    sum(case when array_contains(events, 'harsh_braking') then 1 else 0 end) as harsh_braking_count,
    sum(case when array_contains(events, 'harsh_acceleration') then 1 else 0 end) as harsh_acceleration_count,
    sum(case when array_contains(events, 'speeding') then 1 else 0 end) as speeding_readings,
    sum(case when array_contains(events, 'engine_overheating') then 1 else 0 end) as overheating_readings,
    max(case when check_engine_light then 1 else 0 end) = 1 as had_check_engine_light

from {{ ref('stg_vehicle_telemetry') }}

group by vehicle_id, event_date
