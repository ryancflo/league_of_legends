WITH source AS (
    SELECT
        *
    FROM {{ ref('stg_champions') }}
),

