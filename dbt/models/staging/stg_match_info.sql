WITH source AS(
    SELECT *
    FROM {{ source('league_of_legends_data', 'staging_match_info_stage') }}
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