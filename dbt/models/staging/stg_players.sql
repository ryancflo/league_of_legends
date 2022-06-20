WITH source AS(
    SELECT *
    FROM {{ source('match_stage_data', 'staging_players') }}
),

stg_players as ( 
SELECT
    json_data:summonerId as summonerId,
    json_data:summonerName as summoner_name,
    json_data:leagueId as league_id,
    json_data:inactive as inactive,
    json_data:queueType as queue_type,
    json_data:rank as rank,
    json_data:tier as tier,
    json_data:veteran as veteran,
    json_data:leaguePoints as league_points,
    json_data:wins as wins,
    json_data:losses as losses,
    json_data:freshBlood as fresh_blood,
    json_data:hotStreak as hot_streak
FROM source
)

SELECT *
FROM stg_players