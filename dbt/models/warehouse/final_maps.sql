WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_maps') }}
),

final_datadragon_maps as (
SELECT 
    map_id,
    map_name
FROM source
)

SELECT *
FROM final_datadragon_maps