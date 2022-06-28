{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='matchId'
    )
}}

WITH source AS(
    SELECT *
    FROM {{ ref('stg_match_info') }}
),

final_match_info as (
    SELECT
        matchId,
        gameEndTimestamp,
        gameId,
        gameMode,
        gameName,
        gameStartTimestamp,
        gameType,
        gameVersion,
        mapId,
        platformId,
        queueId,
        tournamentCode
    FROM source
)

SELECT * FROM final_match_info
