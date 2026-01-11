{{ config(materialized='view') }}

SELECT
    CAST(id AS TEXT) AS vehicle_id,
    CAST("routeId" AS TEXT) AS route_id,
    CAST("tripId" AS TEXT) AS trip_id,
    CAST(name AS TEXT) AS vehicle_name,
    CAST(color AS TEXT) AS vehicle_color,
    CAST(type AS TEXT) AS vehicle_type,
    
    -- Decimals should be cast to FLOAT first
    CAST(longitude AS FLOAT) AS longitude,
    
    -- FIXED: Cast these to FLOAT to handle decimals (e.g. 12.5 mph or 312.6 degrees)
    CAST(speed AS FLOAT) AS speed_mph,
    CAST("calculatedCourse" AS FLOAT) AS heading,
    
    -- Passengers are usually whole numbers, but let's be safe and use FLOAT
    -- or cast to float then integer: CAST(CAST("paxLoad" AS FLOAT) AS INTEGER)
    -- For now, FLOAT is safest to prevent crashes.
    CAST("paxLoad" AS FLOAT) AS passenger_load,

    load_timestamp AS created_at

FROM {{ source('raw_data', 'raw_vehicles') }}
WHERE "outOfService" = 0