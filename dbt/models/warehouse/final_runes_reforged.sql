WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_runes_reforged') }}
),

final_datadragon_runes_reforged as (
    SELECT
        rune_id,
        rune_name
    FROM source
)

SELECT *
FROM final_datadragon_runes_reforged