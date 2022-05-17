WITH source AS(
    SELECT *
    FROM {{ source('league_of_legends_data', 'staging_datadragon_champions') }},
    LATERAL FLATTEN(input => json_data)
),

stg_datadragon_champions as (
    SELECT
        key as champion_id,
        value:blurb as blurb,
        value:info:attack as attack,
        value:info:defense as defense,
        value:info:magic as magic,
        value:info:difficulty as difficulty,
        value:stats:hp as hp,
        value:stats:hpperlevel as hpperlevel,
        value:stats:mp as mp,
        value:stats:mpperlevel as mpperlevel,
        value:stats:movespeed as movespeed,
        value:stats:armor as armor,
        value:stats:armorperlevel as armorperlevel,
        value:stats:spellblock as spellblock,
        value:stats:spellblockperlevel as spellblockperlevel,
        value:stats:attackrange as attackrange,
        value:stats:hpregen as hpregen,
        value:stats:hpregenperlevel as hpregenperlevel,
        value:stats:mpregen as mpregen,
        value:stats:mpregenperlevel as mpregenperlevel,
        value:stats:crit as crit,
        value:stats:critperlevel as critperlevel,
        value:stats:attackdamage as attackdamage,
        value:stats:attackdamageperlevel as attackdamageperlevel,
        value:stats:attackspeedperlevel as attackspeedperlevel,
        value:stats:attackspeed as attackspeed
    FROM source
)

SELECT *
FROM stg_datadragon_champions