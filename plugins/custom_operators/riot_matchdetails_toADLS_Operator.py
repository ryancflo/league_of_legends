from datetime import datetime, timedelta
import logging
from os import path 
import tempfile
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_hooks.riot_hook import riotHook

class riot_matchDetailsToADLSOperator(BaseOperator):
    """
    Twitter To Azure DataLake Operator
    :param twitter_conn_id:             The source twitter connection id.
    :type azure_conn_id::               string
    :param azure_conn_id::              The destination azure connection id.
    :type azure_conn_id::               string
    :param azure_bucket:                The destination azure bucket.
    :type azure_bucket:                 string
    :param azure_key:                   The destination azure key.
    :type azure_key:                    string
    :param start_epoch:                 start_time of the query.
    :type start_epoch:                  string
    :param end_epoch:                   end_time of the query.
    :type end_epoch:                    string
    :type limit:                        string
    """
    @apply_defaults
    def __init__(self,
                 riot_conn_id: str,
                 azure_blob: str,
                 azure_conn_id: str,
                 region: str,
                 summoner_name: str,
                 count: int = None,
                 queue: int = None,
                 match_type: str = None,
                 start_epoch: int = None,
                 end_epoch: int = None,
                 ignore_headers=1,
                 *args, **kwargs):

        super(riot_matchDetailsToADLSOperator, self).__init__(*args, **kwargs)

        self.riot_conn_id = riot_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.region = region
        self.summoner_name = summoner_name
        self.count = count
        self.queue = queue
        self.match_type = match_type
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.wasb_hook = WasbHook(self.azure_conn_id)
        self.riot_hook = riotHook(self.riot_conn_id)
        

    def execute(self, context):
        self.log.info('StageToAzureLake not implemented yet')
        self.upload_to_azureLake()
        self.log.info("Upload twitter data to Azure!")

    def upload_to_azureLake(self):
        #Create Azure Connection
        self.log.info(self.wasb_hook.get_conn)
        self.log.info("Created Azure Connection")

        #Create Riot Connection
        self.log.info(self.wasb_hook.get_conn)
        self.log.info("Created Riot Connection")
        
        match_history = self.riot_hook.get_latest_match_history(                 
            self.region,
            self.summoner_name,
            self.count,
            self.queue,
            self.match_type,
            self.start_epoch,
            self.end_epoch
        )
        sub_df = pd.DataFrame(index=[1])
        for match in match_history:
            match = riot_hook.get_match_byid(my_region, match_type)
            match_details = match['info']['participants']
            for dets in match_details:
                challenges = dets.pop('challenges', None)
                perks = dets.pop('perks', None)
                sub_df2 = pd.DataFrame(dets, index=[1])
                sub_df = pd.concat([sub_df, sub_df2], ignore_index = True, axis = 0)


        # Write tweet data to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "match_details.csv")
            sub_df.to_csv(tmp_path, header=True, index=False, columns=list(sub_df.axes[1]))

            # Upload file to Azure Blob.
            wasb_hook.load_file(
                tmp_path,
                container_name="leagueoflegends",
                blob_name="match_details/{year}/{month}/{day}/{hour}.csv".format(
                    year=self.end_epoch.year,
                    month=self.end_epoch.month,
                    day=self.end_epoch.day,
                    hour=self.end_epoch.hour)
                )
