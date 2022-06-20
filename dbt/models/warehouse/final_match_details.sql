{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='matchId'
    )
}}

WITH source AS(
    SELECT *
    FROM {{ source('stage_data', 'stg_match_details') }}
),

final_match_details as (
    SELECT 
        matchId,
        assists,
        baronKills,
        bountyLevel,
        champExperience,
        champLevel,
        championId,
        championName,
        championTransform,
        consumablesPurchased,
        damageDealtToBuildings,
        damageDealtToObjectives,
        damageDealtToTurrets,
        damageSelfMitigated,
        deaths,
        detectorWardsPlaced,
        doubleKills,
        dragonKills,
        eligibleForProgression,
        firstBloodAssist,
        firstBloodKill,
        firstTowerAssist,
        firstTowerKill,
        gameEndedInEarlySurrender,
        gameEndedInSurrender,
        goldEarned,
        goldSpent,
        individualPosition,
        inhibitorKills,
        inhibitorTakedowns,
        inhibitorsLost,
        item0_damage,
        item1_damage,
        item2_damage,
        item3_damage,
        item4_damage,
        item5_damage,
        item6_damage,
        itemsPurchased,
        killingSprees,
        kills,
        lane,
        largestCriticalStrike,
        largestKillingSpree,
        largestMultiKill,
        longestTimeSpentLiving,
        magicDamageDealt,
        magicDamageDealtToChampions,
        magicDamageTaken,
        neutralMinionsKilled,
        nexusKills,
        nexusLost,
        nexusTakedowns,
        objectivesStolen,
        objectivesStolenAssists,
        participantId,
        pentaKills,
        physicalDamageDealt,
        physicalDamageDealtToChampions,
        physicalDamageTaken,
        profileIcon,
        puuid,
        quadraKills,
        riotIdName,
        riotIdTagline,
        role,
        sightWardsBoughtInGame,
        spell1Casts,
        spell2Casts,
        spell3Casts,
        spell4Casts,
        summoner1Casts,
        summoner1Id,
        summoner2Casts,
        summoner2Id,
        summonerId,
        summonerLevel,
        summonerName,
        teamEarlySurrendered,
        teamId,
        teamPosition,
        timeCCingOthers,
        timePlayed,
        totalDamageDealt,
        totalDamageDealtToChampions,
        totalDamageShieldedOnTeammates,
        totalDamageTaken,
        totalHeal,
        totalHealsOnTeammates,
        totalMinionsKilled,
        totalTimeCCDealt,
        totalTimeSpentDead,
        totalUnitsHealed,
        tripleKills,
        trueDamageDealt,
        trueDamageDealtToChampions,
        trueDamageTaken,
        turretKills,
        turretTakedowns,
        turretsLost,
        unrealKills,
        visionScore,
        visionWardsBoughtInGame,
        wardsKilled,
        wardsPlaced,
        win
    FROM source
    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      WHERE matchId > (SELECT max(matchId) from {{ this }})
    {% endif %}
)

SELECT * FROM final_match_details