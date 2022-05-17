-- CREATE OR REPLACE TABLE STAGING_DATADRAGON_ITEMS
-- (
--     json_data variant
-- );

-- COPY INTO "STAGING_DATADRAGON_ITEMS"
-- FROM @MY_AZURE_DATADRAGON_ITEMS_STAGE
-- file_format = datadragon_fileformat;


{% SET pivot_cols = dbt_utils.get_column_values(table=pivot_source, column='KEY') %}

WITH tbl_items AS
(
SELECT 
    key as champion_id,
    value:name as name,
    value:plaintext as plaintext,
    value:gold:base as gold_base,
    value:gold:purchasable as gold_purchasable,
    CASE WHEN value:stats = {} THEN NULL ELSE value:stats END AS stats,
    CASE WHEN value:effect = {} THEN NULL ELSE value:effect END AS effects
FROM
  {{ source('league_of_legends_data', 'staging_datadragon_champions') }},
  LATERAL FLATTEN(input => json_data)
),

pivot_source AS
(
SELECT
    champion_id,
    key,
    value
FROM tbl_items, 
table(flatten(input => parse_json(stats))) as flattened
),

tbl_stats AS (
SELECT * 
FROM pivot_source
    PIVOT( MAX(VALUE) for KEY in (
        {% for pivot_col in pivot_cols %}
            '{{pivot_col}}'{% if not loop.last %}, {% endif%}
        {% endfor %}
        ))
),

tbl_effects AS
(
SELECT 
    champion_id,
    CASE WHEN effects:Effect1Amount IS NULL THEN NULL ELSE effects:Effect1Amount END AS effect1,
    CASE WHEN effects:Effect2Amount IS NULL THEN NULL ELSE effects:Effect2Amount END AS effect2,
    CASE WHEN effects:Effect3Amount IS NULL THEN NULL ELSE effects:Effect3Amount END AS effect3,
    CASE WHEN effects:Effect4Amount IS NULL THEN NULL ELSE effects:Effect4Amount END AS effect4,
    CASE WHEN effects:Effect5Amount IS NULL THEN NULL ELSE effects:Effect5Amount END AS effect5,
    CASE WHEN effects:Effect6Amount IS NULL THEN NULL ELSE effects:Effect6Amount END AS effect6,
    CASE WHEN effects:Effect7Amount IS NULL THEN NULL ELSE effects:Effect7Amount END AS effect7,
    CASE WHEN effects:Effect8Amount IS NULL THEN NULL ELSE effects:Effect8Amount END AS effect8,
    CASE WHEN effects:Effect9Amount IS NULL THEN NULL ELSE effects:Effect9Amount END AS effect9,
    CASE WHEN effects:Effect10Amount IS NULL THEN NULL ELSE effects:Effect10Amount END AS effect10,
    CASE WHEN effects:Effect11Amount IS NULL THEN NULL ELSE effects:Effect11Amount END AS effect11,
    CASE WHEN effects:Effect12Amount IS NULL THEN NULL ELSE effects:Effect12Amount END AS effect12,
    CASE WHEN effects:Effect13Amount IS NULL THEN NULL ELSE effects:Effect13Amount END AS effect13,
    CASE WHEN effects:Effect14Amount IS NULL THEN NULL ELSE effects:Effect14Amount END AS effect14,
    CASE WHEN effects:Effect15Amount IS NULL THEN NULL ELSE effects:Effect15Amount END AS effect15,
    CASE WHEN effects:Effect16Amount IS NULL THEN NULL ELSE effects:Effect16Amount END AS effect16,
    CASE WHEN effects:Effect17Amount IS NULL THEN NULL ELSE effects:Effect17Amount END AS effect17,
    CASE WHEN effects:Effect16Amount IS NULL THEN NULL ELSE effects:Effect18Amount END AS effect18
FROM tbl_items
),

stg_datadragon_items AS(

)

SELECT
    *
FROM tbl_items 
    LEFT JOIN tbl_effects 
        ON tbl_items.champion_id = tbl.effects.champion_id 
    LEFT JOIN tbl_stats 
        ON tbl_items.champion_id = tbl.stats.champion_id 



