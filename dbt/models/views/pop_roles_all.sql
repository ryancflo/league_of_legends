WITH source AS (
  SELECT
    teamPosition,
    COUNT(teamPosition) as count_teamPosition
  FROM {{ source('league_final_tables', 'final_match_details') }}
  GROUP BY teamPosition
)

SELECT * FROM source