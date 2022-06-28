WITH augment_matches AS (
SELECT md.matchId,
       md.summonerName,
       mi.gamemode AS map,
       md.deaths,
       md.kills,
       md.win,
       mi.gameEndTimestamp AS completion_date,
       md.teamId
FROM {{ source('league_final_tables', 'final_match_details') }} md
JOIN {{ source('league_final_tables', 'final_match_info') }} mi ON md.matchId = mi.matchId
JOIN {{ source('league_final_tables', 'final_players') }} mp ON md.summonerName = mp.summonerName
WHERE mp.veteran = false
),
lagged AS (
    SELECT 
        *, 
        LAG(win, 0) OVER (PARTITION BY summonerName, map ORDER BY completion_date) AS did_win
    FROM augment_matches
),
streak_change AS(
  SELECT 
  *,
  CASE WHEN win <> did_win THEN 1 else 0 END as streak_changed
  FROM lagged
),
streak_identified AS(
    SELECT *,
        SUM(streak_changed) OVER (PARTITION BY summonerName, map ORDER BY completion_date) AS streak_identifier
    FROM streak_change
),
record_counts AS(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY summonerName, map, streak_identifier ORDER BY completion_date) AS streak_length
    FROM streak_identified
),
ranked AS (
SELECT*,
RANK() OVER (PARTITION BY summonerName, map, streak_identifier ORDER BY streak_length DESC) AS rank
FROM record_counts
)

SELECT * FROM ranked
WHERE rank = 1