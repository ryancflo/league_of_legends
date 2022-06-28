//Damage ratio
WITH top_5 AS(
  SELECT
    matchId,
    summonerName,
    teamId,
    streak_length
  FROM {{ ref('smurfs_analysis') }}
  ORDER BY streak_length DESC, summonerName 
LIMIT 5
),
top_5_kda AS (
  SELECT
    t5.matchId as matchId,
    t5.summonerName as player,
    md.summonerName as teamPlayer,
    md.kills as teamPlayerKills, 
    md.deaths teamPlayerDeaths,
    md.assists teamPlayerAssists,
    md2.kills playerKills,
    md2.deaths playerDeaths,
    md2.assists playerAssists,
    md.kills / SUM(md.kills) OVER (PARTITION BY md.matchId, md2.summonerName) as teamPlayer_kd_ratio,
    md2.kills / SUM(md.kills) OVER (PARTITION BY md.matchId, md2.summonerName) as player_kd_ratio,
    (CASE WHEN md2.kills >= MAX(md.kills) OVER (PARTITION BY md.matchId) THEN True ELSE false END) as highest_kills,
    md.totalDamageDealtToChampions as teamPlayer_dmgToChampions,
    md2.totalDamageDealtToChampions as player_dmgToChampions,
    md.totalDamageDealtToChampions / SUM(md.totalDamageDealtToChampions) OVER (PARTITION BY md.matchId, md2.summonerName) as teamPlayer_dmgToChampions_ratio,
    md2.totalDamageDealtToChampions / SUM(md.totalDamageDealtToChampions) OVER (PARTITION BY md.matchId, md2.summonerName) as player_dmgToChampions_ratio,
    (CASE WHEN md2.totalDamageDealtToChampions >= MAX(md.totalDamageDealtToChampions) OVER (PARTITION BY md.matchId) THEN True ELSE false END) as highest_damage,
    md.goldEarned as teamPlayer_goldEarned,
    md2.goldEarned as player_goldEarned,
    md.goldEarned / SUM(md.goldEarned) OVER (PARTITION BY md.matchId, md2.summonerName) as teamPlayer_goldEarned_ratio,
    md2.goldEarned / SUM(md2.goldEarned) OVER (PARTITION BY md.matchId, md2.summonerName) as player_goldEarned_ratio,
    (CASE WHEN md2.goldEarned  >= MAX(md.goldEarned ) OVER (PARTITION BY md.matchId) THEN True ELSE false END) as highest_gold,
    md.teamPosition as teamPlayer_role,
    md2.teamPosition player_role
  FROM top_5 t5
  LEFT JOIN {{ source('league_final_tables', 'final_match_details') }} md ON t5.matchId = md.matchId AND t5.teamId = md.teamId
  LEFT JOIN {{ source('league_final_tables', 'final_match_details') }} md2 ON t5.matchId = md2.matchId AND t5.summonerName = md2.summonerName
  ORDER BY t5.summonerName
)

SELECT
*
FROM top_5_kda 