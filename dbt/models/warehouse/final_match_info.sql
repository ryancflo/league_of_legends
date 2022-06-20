{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='matchId'
    )
}}

WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_match_info') }}
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
    {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
      WHERE matchId > (SELECT max(matchId) from {{ this }})
    {% endif %}
)

SELECT * FROM final_match_info
