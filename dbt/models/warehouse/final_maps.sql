WITH source AS(
    SELECT *
    FROM {{ ref('stg_maps') }}
),

final_datadragon_maps as (
SELECT 
    map_id,
    map_name
FROM source
)

SELECT *
FROM final_datadragon_maps