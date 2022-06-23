from datetime import datetime, timedelta
import logging
from os import path 
import tempfile
import pandas as pd
import json
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.riot_hook import riotHook

class riot_matchDetailsToADLSOperator(BaseOperator):
    """
    Riot To Azure DataLake Operator
    :param riot_conn_id:             The source Riot connection id.
    :type riot_conn_id::               string
    :param azure_key:                   The destination azure key.
    :type azure_key:                    string
    :param start_epoch:                 start_time of the query.
    :type start_epoch:                  string
    :param end_epoch:                   end_time of the query.
    :type end_epoch:                    string
    """
    @apply_defaults
    def __init__(self,
                 riot_conn_id: str,
                 azure_conn_id: str,
                 region: str,
                 match_queue: str,
                 summoner_name: str,
                 tier: str = None,
                 division: str = None,
                 page: int = None,
                 count: int = None,
                 player_count: int = None,
                 queue_type: str = None,
                 start_epoch: int = None,
                 end_epoch: int = None,
                 ignore_headers=1,
                 *args, **kwargs):

        super(riot_matchDetailsToADLSOperator, self).__init__(*args, **kwargs)

        self.riot_conn_id = riot_conn_id
        self.azure_conn_id = azure_conn_id
        self.region = region
        self.match_queue = match_queue
        self.summoner_name = summoner_name
        self.tier = tier
        self.division = division
        self.page = page
        self.count = count
        self.player_count = player_count
        self.queue_type = queue_type
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch


        

    def execute(self, context):
        self.log.info('StageToAzureLake not implemented yet')
        self.upload_to_azureLake()
        self.log.info("Upload match data to Azure!")



    def upload_to_azureLake(self):
        #Create Azure Connection
        wasb_hook = WasbHook(self.azure_conn_id)
        self.log.info(wasb_hook.get_conn)
        self.log.info("Created Azure Connection")

        riot_hook = riotHook(self.riot_conn_id)
        self.log.info("Created Riot Connection")
        # Fetch top 10 Challenger players
        players = riot_hook.get_leagues(self.region, self.match_queue, self.tier, self.division, self.page)
        players.sort(key=lambda x: x['leaguePoints'], reverse=True)
        top10_players = players[:self.player_count]
        print(top10_players)

        #Create Empty dfs
        sub_df = pd.DataFrame()
        match_info_df = pd.DataFrame()

        for player in top10_players:
            summoner = riot_hook.get_summoner_byid(self.region, player['summonerId'])
            matches = riot_hook.get_matchlist_by_puuid(self.region, summoner['puuid'], count=self.count, queue_type=self.queue_type)

            for match in matches:
                match = riot_hook.get_match_byid(self.region, match)
                
                # sub_df = pd.concat([sub_df, match_details_df], ignore_index = True, axis = 0)
                match_details = match['info']['participants']

                match_info = match['info']
                match_participants = match_info.pop('participants')
                match_team_info = match_info.pop('teams')
                match_details_df = pd.DataFrame({'matchId': [match['metadata']['matchId']]})

                match_info_df_sub = pd.DataFrame([match_info])
                match_info_df_1 = pd.concat([match_details_df, match_info_df_sub], axis = 1)
                match_info_df = pd.concat([match_info_df, match_info_df_1], axis = 0)
                
                
                for dets in match_details:
                    challenges = dets.pop('challenges', None)
                    perks = dets.pop('perks', None)
                    
                    match_details_df = pd.DataFrame({'matchId': [match['metadata']['matchId']]})

                    sub_df2 = pd.DataFrame([dets])
                    # print(sub_df2)
                    # print(match_details_df)
                    sub_df1 = pd.concat([match_details_df, sub_df2], axis = 1)
                    sub_df = pd.concat([sub_df, sub_df1], axis = 0)
                    # sub_df['matchId'] = match['metadata']['matchId']

            # print(match_info_df)
        # Write Riot data to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "match_details.csv")
            tmp_path2 = path.join(tmp_dir, "match_info.csv")
            # tmp_path3 = path.join(tmp_dir, "top10_players.json")
            sub_df.to_csv(tmp_path, header=True, index=False, columns=list(sub_df.axes[1]))
            match_info_df.to_csv(tmp_path2, header=True, index=False, columns=list(match_info_df.axes[1]))
            # match_info_df.to_csv(tmp_path3, header=True, index=False, columns=list(match_info_df.axes[1]))

            self.log.info(tmp_path)
            wasb_hook.load_file(
                tmp_path,
                container_name="match-details",
                blob_name="{year}/{month}/{day}/{hour}.csv".format(
                    year=self.end_epoch.year,
                    month=self.end_epoch.month,
                    day=self.end_epoch.day,
                    hour=self.end_epoch.hour)
                )
            self.log.info("match-details loaded")

            self.log.info(tmp_path2)
            wasb_hook.load_file(
                tmp_path2,
                container_name="match-info",
                blob_name="{year}/{month}/{day}/{hour}.csv".format(
                    year=self.end_epoch.year,
                    month=self.end_epoch.month,
                    day=self.end_epoch.day,
                    hour=self.end_epoch.hour)
                )
            self.log.info("match-info loaded")


        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path3 = path.join(tmp_dir, "top10_players.json")
            
            with open(tmp_path3, 'w') as fp:
                json.dump(top10_players, fp)
            with open(tmp_path3, 'r') as fp:
                print(fp.read())
                

                wasb_hook.load_file(
                    tmp_path3,
                    container_name="players",
                    blob_name="{year}/{month}/{day}/{hour}.json".format(
                        year=self.end_epoch.year,
                        month=self.end_epoch.month,
                        day=self.end_epoch.day,
                        hour=self.end_epoch.hour)
                    )
                self.log.info("players loaded")




