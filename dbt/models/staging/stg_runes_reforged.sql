-- CREATE OR REPLACE TABLE STAGING_DATADRAGON_RUNES_REFORGED
-- (
--     json_data variant
-- );

-- COPY INTO "STAGING_DATADRAGON_RUNES_REFORGED"
-- FROM @MY_AZURE_DATADRAGON_RUNESREFORGED_STAGE
-- file_format = datadragon_fileformat;

WITH tbl_runes AS
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
    ID,
    f1.value:key as rune_name
FROM tbl,
    LATERAL FLATTEN(input => value_extract) f,
    LATERAL FLATTEN(input => value:runes) f1
