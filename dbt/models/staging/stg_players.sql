WITH source AS(
    SELECT *
    FROM {{ source('match_stage_data', 'staging_players') }}
),

stg_players as ( 
SELECT
    json_data:summonerId::varchar as summonerId,
    json_data:summonerName::varchar as summonerName,
    json_data:leagueId::varchar as league_id,
    json_data:inactive::boolean as inactive,
    json_data:queueType::varchar as queue_type,
    json_data:rank::string as rank,
    json_data:tier::string as tier,
    json_data:veteran::boolean as veteran,
    json_data:leaguePoints::integer as league_points,
    json_data:wins::integer as wins,
    json_data:losses::integer as losses,
    json_data:freshBlood::boolean as fresh_blood,
    json_data:hotStreak::boolean as hot_streak
FROM source
)

SELECT *
FROM stg_players