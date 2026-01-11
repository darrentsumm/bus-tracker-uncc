{{ config(materialized='table') }}

SELECT
    v.vehicle_id,
    v.vehicle_name,
    v.vehicle_color,
    r.route_name,
    r.route_short_name,
    v.longitude,
    v.speed_mph,
    v.heading,
    v.passenger_load,
    v.created_at

FROM {{ ref('stg_vehicles') }} AS v

LEFT JOIN {{ ref('stg_routes') }} AS r
    ON v.route_id = r.route_id