WITH source AS(
    SELECT *
    FROM {{ ref('stg_runes_reforged') }}
),

final_datadragon_runes_reforged as (
    SELECT
        rune_treeid,
        rune_name
    FROM source
    -- {% if is_incremental() %}
    --   -- this filter will only be applied on an incremental run
    --   WHERE matchId > (SELECT max(matchId) from {{ this }})
    -- {% endif %}
)

SELECT *
FROM final_datadragon_runes_reforged