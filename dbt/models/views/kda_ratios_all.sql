WITH all_kda AS (
  SELECT
    AVG(kills) avg_kills,
    AVG(deaths) avg_deaths,
    AVG(assists) avg_assists,
    ROUND(AVG(kills)/iff(AVG(deaths) = 0, 1, AVG(deaths)),2) as kd_ratio,
    ROUND(AVG(kills + assists)/iff(AVG(deaths) = 0, 1, AVG(deaths)),2) as kda_ratio
  FROM {{ ref('final_match_details') }}
)

SELECT * FROM all_kda
