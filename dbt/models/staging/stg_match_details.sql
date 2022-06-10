WITH source AS(
    SELECT *
    FROM {{ source('league_of_legends_data', 'staging_match_details') }}
),


stg_match_details as (
    SELECT 
        matchId as matchId,
        assists as assists,
        baronKills as baronKills,
        bountyLevel as bountyLevel,
        champExperience as champExperience,
        champLevel as champLevel,
        championId as championId,
        championName as championName,
        championTransform as championTransform,
        consumablesPurchased as consumablesPurchased,
        damageDealtToBuildings as damageDealtToBuildings,
        damageDealtToObjectives as damageDealtToObjectives,
        damageDealtToTurrets as damageDealtToTurrets,
        damageSelfMitigated as damageSelfMitigated,
        deaths as deaths,
        detectorWardsPlaced as detectorWardsPlaced,
        doubleKills as doubleKills,
        dragonKills as dragonKills,
        eligibleForProgression as eligibleForProgression,
        firstBloodAssist as firstBloodAssist,
        firstBloodKill as firstBloodKill,
        firstTowerAssist as firstTowerAssist,
        firstTowerKill as firstTowerKill,
        gameEndedInEarlySurrender as gameEndedInEarlySurrender,
        gameEndedInSurrender as gameEndedInSurrender,
        goldEarned as goldEarned,
        goldSpent as goldSpent,
        individualPosition as individualPosition,
        inhibitorKills as inhibitorKills,
        inhibitorTakedowns as inhibitorTakedowns,
        inhibitorsLost as inhibitorsLost,
        item0 as item0_damage,
        item1 as item1_damage,
        item2 as item2_damage,
        item3 as item3_damage,
        item4 as item4_damage,
        item5 as item5_damage,
        item6 as item6_damage,
        itemsPurchased as itemsPurchased,
        killingSprees as killingSprees,
        kills as kills,
        lane as lane,
        largestCriticalStrike as largestCriticalStrike,
        largestKillingSpree as largestKillingSpree,
        largestMultiKill as largestMultiKill,
        longestTimeSpentLiving as longestTimeSpentLiving,
        magicDamageDealt as magicDamageDealt,
        magicDamageDealtToChampions as magicDamageDealtToChampions,
        magicDamageTaken as magicDamageTaken,
        neutralMinionsKilled as neutralMinionsKilled,
        nexusKills as nexusKills,
        nexusLost as nexusLost,
        nexusTakedowns as nexusTakedowns,
        objectivesStolen as objectivesStolen,
        objectivesStolenAssists as objectivesStolenAssists,
        participantId as participantId,
        pentaKills as pentaKills,
        physicalDamageDealt as physicalDamageDealt,
        physicalDamageDealtToChampions as physicalDamageDealtToChampions,
        physicalDamageTaken as physicalDamageTaken,
        profileIcon as profileIcon,
        puuid as puuid,
        quadraKills as quadraKills,
        riotIdName as riotIdName,
        riotIdTagline as riotIdTagline,
        role as role,
        sightWardsBoughtInGame as sightWardsBoughtInGame,
        spell1Casts as spell1Casts,
        spell2Casts as spell2Casts,
        spell3Casts as spell3Casts,
        spell4Casts as spell4Casts,
        summoner1Casts as summoner1Casts,
        summoner1Id as summoner1Id,
        summoner2Casts as summoner2Casts,
        summoner2Id as summoner2Id,
        summonerId as summonerId,
        summonerLevel as summonerLevel,
        summonerName as summonerName,
        teamEarlySurrendered as teamEarlySurrendered,
        teamId as teamId,
        teamPosition as teamPosition,
        timeCCingOthers as timeCCingOthers,
        timePlayed as timePlayed,
        totalDamageDealt as totalDamageDealt,
        totalDamageDealtToChampions as totalDamageDealtToChampions,
        totalDamageShieldedOnTeammates as totalDamageShieldedOnTeammates,
        totalDamageTaken as totalDamageTaken,
        totalHeal as totalHeal,
        totalHealsOnTeammates as totalHealsOnTeammates,
        totalMinionsKilled as totalMinionsKilled,
        totalTimeCCDealt as totalTimeCCDealt,
        totalTimeSpentDead as totalTimeSpentDead,
        totalUnitsHealed as totalUnitsHealed,
        tripleKills as tripleKills,
        trueDamageDealt as trueDamageDealt,
        trueDamageDealtToChampions as trueDamageDealtToChampions,
        trueDamageTaken as trueDamageTaken,
        turretKills as turretKills,
        turretTakedowns as turretTakedowns,
        turretsLost as turretsLost,
        unrealKills as unrealKills,
        visionScore as visionScore,
        visionWardsBoughtInGame as visionWardsBoughtInGame,
        wardsKilled as wardsKilled,
        wardsPlaced as wardsPlaced,
        win as win
    FROM source
)

SELECT *
FROM stg_match_details