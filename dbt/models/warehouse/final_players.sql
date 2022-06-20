{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='summonerId'
    )
}}

WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_players') }}
),

final_players as (
    SELECT
        summonerId,
        summoner_name,
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
    {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
      WHERE summonerId > (SELECT max(summonerId) from {{ this }})
    {% endif %}
)

SELECT *
FROM final_players