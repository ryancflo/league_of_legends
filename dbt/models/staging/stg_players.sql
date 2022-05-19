-- CREATE OR REPLACE TABLE STAGING_PLAYERS
-- (
--     json_data variant
-- );

-- COPY INTO "STAGING_PLAYERS"
-- FROM @MY_AZURE_PLAYERS
-- file_format = datadragon_fileformat;

WITH source AS(
    SELECT *
    FROM {{ source('league_of_legends_data', 'staging_players_stage') }}
),

stg_players AS (
    SELECT 
        f1.index,
        f1.value:summonerId as summoner_id,
        f1.value:summonerName as summonerName,
        f1.value:leaguePoints as league_points,
        f1.value:veteran as veteran,
        f1.value:wins as wins,
        f1.value:losses as losses,
        f1.value:inactive as inactive,
        f1.value:hotStreak as hotStreak,
        f1.value:veteran as Veteran
    FROM source,
        LATERAL FLATTEN(input => json_data) f,
        LATERAL FLATTEN(input => value) f1
)

SELECT *
FROM stg_players