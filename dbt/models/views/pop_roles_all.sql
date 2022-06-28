WITH source AS (
  SELECT
    teamPosition,
    COUNT(teamPosition) as count_teamPosition
  FROM {{ ref('final_match_details') }}
  GROUP BY teamPosition
)

SELECT * FROM source