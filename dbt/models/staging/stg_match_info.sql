WITH source AS(
    SELECT *
    FROM {{ source('match_stage_data', 'staging_matchinfo') }}
),


stg_match_info as (
    SELECT
        matchId as matchId,
        gameEndTimestamp as gameEndTimestamp,
        gameId as gameId,
        gameMode as gameMode,
        gameName as gameName,
        gameStartTimestamp as gameStartTimestamp,
        gameType as gameType,
        gameVersion as gameVersion,
        mapId as mapId,
        platformId as platformId,
        queueId as queueId,
        tournamentCode as tournamentCode
    FROM source
)

SELECT *
FROM stg_match_info