{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='summonerId'
    )
}}

WITH source AS(
    SELECT *
    FROM {{ ref('stg_players') }}
),

final_players as (
    SELECT
        summonerId,
        summonerName,
        league_id,
        inactive,
        queue_type,
        rank,
        tier,
        veteran,
        league_points,
        wins,
        losses,
        fresh_blood,
        hot_streak
    FROM source
)

SELECT *
FROM final_players