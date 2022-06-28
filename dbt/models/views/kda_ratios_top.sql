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
    md.summonerName,
    AVG(md.kills) avg_kills,
    AVG(md.deaths) avg_deaths,
    AVG(md.assists) avg_assists,
    ROUND(AVG(md.kills)/iff(AVG(md.deaths) = 0, 1, AVG(md.deaths)),2) as kd_ratio,
    ROUND(AVG(md.kills + md.assists)/iff(AVG(md.deaths) = 0, 1, AVG(md.deaths)),2) as kda_ratio
  FROM top_5 t5
  LEFT JOIN {{ ref('final_match_details') }} md ON t5.summonerName = md.summonerName
  GROUP BY md.summonerName
)

SELECT * FROM top_5_kda
