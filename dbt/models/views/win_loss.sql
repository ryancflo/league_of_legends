WITH top_5 AS(
  SELECT
    summonerName,
    streak_length
  FROM {{ ref('smurfs_analysis') }}
  ORDER BY streak_length DESC, summonerName 
LIMIT 5
),
top_5_kda AS (
  SELECT
    t5.summonerName as summonerName,
    mp.wins as wins,
    mp.losses as losses
  FROM top_5 t5
  LEFT JOIN {{ source('league_final_tables', 'final_players') }} mp ON t5.summonerName = mp.summonerName
)

SELECT * FROM top_5_kda
ORDER BY wins
LIMIT 5 