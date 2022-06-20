WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_champions') }}
),

final_datadragon_champions as (
    SELECT
        champion_id,
        blurb,
        attack,
        defense,
        magic,
        difficulty,
        hp,
        hpperlevel,
        mp,
        mpperlevel,
        movespeed,
        armor,
        armorperlevel,
        spellblock,
        spellblockperlevel,
        attackrange,
        hpregen,
        hpregenperlevel,
        mpregen,
        mpregenperlevel,
        crit,
        critperlevel,
        attackdamage,
        attackdamageperlevel,
        attackspeedperlevel,
        attackspeed
    FROM source
)

SELECT *
FROM final_datadragon_champions