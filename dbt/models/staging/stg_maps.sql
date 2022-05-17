-- CREATE OR REPLACE TABLE STAGING_DATADRAGON_MAPS
-- (
--     json_data variant
-- );

-- COPY INTO "STAGING_DATADRAGON_MAPS"
-- FROM @MY_AZURE_DATADRAGON_MAPS_STAGE
-- file_format = datadragon_fileformat;

WITH source AS(
    SELECT *
    FROM {{ source('league_of_legends_data', 'staging_datadragon_maps') }},
    LATERAL FLATTEN(input => json_data)
),

stg_datadragon_maps as (
SELECT 
    key as map_id,
    value:MapName as map_name
FROM source
)

SELECT *
FROM stg_datadragon_maps