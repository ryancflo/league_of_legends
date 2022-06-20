WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_summonerspells') }}
),

final_datadragon_summonerspells as (
    SELECT
        summonerspell_id,
        cooldown,
        range,
        summoner_level
    FROM source
)

SELECT *
FROM final_datadragon_summonerspells