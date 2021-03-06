-- CREATE OR REPLACE TABLE STAGING_DATADRAGON_SUMMONERSPELLS
-- (
--     json_data variant
-- );

-- COPY INTO "STAGING_DATADRAGON_SUMMONERSPELLS"
-- FROM @MY_AZURE_DATADRAGON_SUMMONERSPELLS_STAGE
-- file_format = datadragon_fileformat;

WITH source AS(
    SELECT *
    FROM {{ source('datadragon_stage_data', 'staging_datadragon_summonerspells') }},
    LATERAL FLATTEN(input => json_data)
),

stg_datadragon_summonerspells as (
SELECT 
    key::varchar as summonerspell_id,
    value:cooldown[0]::integer as cooldown,
    value:range[0]::integer as range,
    value:summonerLevel::integer as summoner_level
FROM source
)

SELECT *
FROM stg_datadragon_summonerspells