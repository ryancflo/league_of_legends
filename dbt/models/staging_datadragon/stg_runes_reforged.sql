WITH source AS(
    SELECT *
    FROM {{ source('datadragon_stage_data', 'staging_datadragon_runes_reforged') }},
    LATERAL FLATTEN(input => json_data)
),

tbl_runes AS
(
SELECT 
    *
FROM
    STAGING_DATADRAGON_RUNES_REFORGED,
    LATERAL FLATTEN(input => json_data)
),
tbl AS
(
SELECT
    Seq as id,
    value as value_extract
FROM tbl_runes
    WHERE key = 'slots'
)

SELECT
    ID::integer as rune_treeid,
    f1.value:key::varchar as rune_name
FROM tbl,
    LATERAL FLATTEN(input => value_extract) f,
    LATERAL FLATTEN(input => value:runes) f1
