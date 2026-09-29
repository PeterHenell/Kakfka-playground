-- The most recent reading of every vehicle: where is it and is it healthy?

select
    vehicle_id,
    vehicle_type,
    recorded_at,
    latitude,
    longitude,
    speed_kmh,
    engine_temp_c,
    fuel_level_pct,
    battery_voltage,
    least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa) as min_tire_pressure_kpa,
    check_engine_light,
    dtc_codes

from {{ ref('stg_vehicle_telemetry') }}

qualify row_number() over (partition by vehicle_id order by recorded_at desc) = 1
