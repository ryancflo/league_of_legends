WITH source AS(
  SELECT
    summonerName
  FROM {{ ref('smurfs_analysis') }}
  ORDER BY streak_length DESC, summonerName 
LIMIT 5
),
roles_top AS (
  SELECT
    md.teamPosition as teamPostition,
    COUNT(*) as count_teamPosition
  FROM source t5
  LEFT JOIN {{ source('league_final_tables', 'final_match_details') }} md ON t5.summonerName = md.summonerName
  GROUP BY md.teamPosition
)
-- pop_roles_top AS (
--   SELECT
--     teamPosition,
--     count
--   FROM roles_top
--   WHERE count = (SELECT MAX(count) FROM roles_top)
-- )


SELECT * FROM roles_top