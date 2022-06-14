-- CREATE OR REPLACE TABLE STAGING_DATADRAGON_ITEMS
WITH tbl_items AS
(
SELECT *
  FROM {{ source('datadragon_stage_data', 'staging_datadragon_items') }},
  LATERAL FLATTEN(input => json_data)
WHERE key = 'data'
),

transform_1 AS (
SELECT
  t.key as key,
  t.value as value
FROM tbl_items,
  LATERAL FLATTEN(input => value, OUTER => TRUE) as t
),

transform_2 AS(
SELECT
  key,
  value:name as name,
  value:description as gold_description,
  value:gold:base as gold_base,
  value:gold:purchasable as gold_purchasable,
  value:gold:total as gold_total,
  value:gold:sell as gold_sell,
  value:stats as stats,
  value:maps as maps
FROM
  transform_1
),

pivot_source AS(
SELECT
  tbl1.key as id,
  name,
  gold_description,
  gold_base,
  gold_purchasable,
  gold_total,
  gold_sell,
  tbl2.KEY as stat_name,
  tbl2.value as stat_value,
  tbl3.KEY as map_id,
  tbl3.value as map_exists
FROM transform_2 as tbl1,
  LATERAL FLATTEN(input => stats, OUTER => TRUE) as tbl2,
  LATERAL FLATTEN(input => maps, OUTER => TRUE) as tbl3
),

pivoted_items AS(
SELECT
  *
FROM pivot_source
pivot(max(map_exists) for map_id in ('11','12','21','22'))
pivot(max(stat_value) for stat_name in (      
    'FlatHPPoolMod',
    'rFlatHPModPerLevel',
    'FlatMPPoolMod',
    'rFlatMPModPerLevel',
    'PercentHPPoolMod',
    'PercentMPPoolMod',
    'FlatHPRegenMod',
    'rFlatHPRegenModPerLevel',
    'PercentHPRegenMod',
    'FlatMPRegenMod',
    'rFlatMPRegenModPerLevel',
    'PercentMPRegenMod',
    'FlatArmorMod',
    'rFlatArmorModPerLevel',
    'PercentArmorMod',
    'rFlatArmorPenetrationMod',
    'rFlatArmorPenetrationModPerLevel',
    'rPercentArmorPenetrationMod',
    'rPercentArmorPenetrationModPerLevel',
    'FlatPhysicalDamageMod',
    'rFlatPhysicalDamageModPerLevel',
    'PercentPhysicalDamageMod',
    'FlatMagicDamageMod',
    'rFlatMagicDamageModPerLevel',
    'PercentMagicDamageMod',
    'FlatMovementSpeedMod',
    'rFlatMovementSpeedModPerLevel',
    'PercentMovementSpeedMod',
    'rPercentMovementSpeedModPerLevel',
    'FlatAttackSpeedMod',
    'PercentAttackSpeedMod',
    'rPercentAttackSpeedModPerLevel',
    'rFlatDodgeMod',
    'rFlatDodgeModPerLevel',
    'PercentDodgeMod',
    'FlatCritChanceMod',
    'rFlatCritChanceModPerLevel',
    'PercentCritChanceMod',
    'FlatCritDamageMod',
    'rFlatCritDamageModPerLevel',
    'PercentCritDamageMod',
    'FlatBlockMod',
    'PercentBlockMod',
    'FlatSpellBlockMod',
    'rFlatSpellBlockModPerLevel',
    'PercentSpellBlockMod',
    'FlatEXPBonus',
    'PercentEXPBonus',
    'rPercentCooldownMod',
    'rPercentCooldownModPerLevel',
    'rFlatTimeDeadMod',
    'rFlatTimeDeadModPerLevel',
    'rPercentTimeDeadMod',
    'rPercentTimeDeadModPerLevel',
    'rFlatGoldPer10Mod',
    'rFlatMagicPenetrationMod',
    'rFlatMagicPenetrationModPerLevel',
    'rPercentMagicPenetrationMod',
    'rPercentMagicPenetrationModPerLevel',
    'FlatEnergyRegenMod',
    'rFlatEnergyRegenModPerLevel',
    'FlatEnergyPoolMod',
    'rFlatEnergyModPerLevel',
    'PercentLifeStealMod',
    'PercentSpellVampMod'
))
ORDER BY id
)

SELECT * FROM pivoted_items



