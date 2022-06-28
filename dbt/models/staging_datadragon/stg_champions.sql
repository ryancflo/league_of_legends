WITH source AS(
    SELECT *
    FROM {{ source('datadragon_stage_data', 'staging_datadragon_champions') }},
    LATERAL FLATTEN(input => json_data)
),

stg_datadragon_champions as (
    SELECT
        key::varchar as champion_id,
        value:blurb::varchar as blurb,
        value:info:attack::integer as attack,
        value:info:defense::integer as defense,
        value:info:magic::integer as magic,
        value:info:difficulty::integer as difficulty,
        value:stats:hp::float as hp,
        value:stats:hpperlevel::float as hpperlevel,
        value:stats:mp::float as mp,
        value:stats:mpperlevel::float as mpperlevel,
        value:stats:movespeed::float as movespeed,
        value:stats:armor::float as armor,
        value:stats:armorperlevel::float as armorperlevel,
        value:stats:spellblock::float as spellblock,
        value:stats:spellblockperlevel::float as spellblockperlevel,
        value:stats:attackrange::float as attackrange,
        value:stats:hpregen::float as hpregen,
        value:stats:hpregenperlevel::float as hpregenperlevel,
        value:stats:mpregen::float as mpregen,
        value:stats:mpregenperlevel::float as mpregenperlevel,
        value:stats:crit::float as crit,
        value:stats:critperlevel::float as critperlevel,
        value:stats:attackdamage::float as attackdamage,
        value:stats:attackdamageperlevel::float as attackdamageperlevel,
        value:stats:attackspeedperlevel::float as attackspeedperlevel,
        value:stats:attackspeed::float as attackspeed
    FROM source
)

SELECT *
FROM stg_datadragon_champions