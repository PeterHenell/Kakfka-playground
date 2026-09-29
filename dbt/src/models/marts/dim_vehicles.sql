-- One row per vehicle. VIN and type are taken from the latest reading.

select
    vehicle_id,
    max_by(vin, recorded_at) as vin,
    max_by(vehicle_type, recorded_at) as vehicle_type,
    min(recorded_at) as first_seen_at,
    max(recorded_at) as last_seen_at,
    count(*) as reading_count,
    max(odometer_km) as odometer_km

from {{ ref('stg_vehicle_telemetry') }}

group by vehicle_id
