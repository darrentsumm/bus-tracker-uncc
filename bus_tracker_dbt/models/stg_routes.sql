{{ config(materialized='view') }}

SELECT
    -- We assume 'myid' and 'id' are lowercase based on standard APIs, 
    -- but 'shortName' and 'groupColor' definitely need quotes.
    CAST(myid AS TEXT) AS route_id,
    CAST(id AS TEXT) AS internal_system_id,
    
    CAST(name AS TEXT) AS route_name,
    
    -- FIXED: Added double quotes to handle camelCase
    CAST("shortName" AS TEXT) AS route_short_name,
    CAST("groupColor" AS TEXT) AS route_color,
    
    load_timestamp AS loaded_at
    
FROM
    {{ source('raw_data', 'raw_routes') }}