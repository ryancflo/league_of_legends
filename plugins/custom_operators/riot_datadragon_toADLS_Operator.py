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

class riot_dataDragonToADLSOperator(BaseOperator):
    """
    Riot To Azure DataLake Operator
    :param riot_conn_id:             The source twitter connection id.
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
                 version: str,
                 data_url: str,
                 end_epoch: int,
                 region: str  = None,
                 ignore_headers=1,
                 *args, **kwargs):

        super(riot_dataDragonToADLSOperator, self).__init__(*args, **kwargs)

        self.riot_conn_id = riot_conn_id
        self.azure_blob = azure_blob
        self.azure_conn_id = azure_conn_id
        self.version = version
        self.data_url = data_url
        self.end_epoch = end_epoch
        self.region = region
        
        

    def execute(self, context):
        self.log.info('StageToAzureLake not implemented yet')
        self.upload_to_azureLake()
        self.log.info("Upload league of legends data to Azure!")

    def upload_to_azureLake(self):
        #Create Azure Connection
        wasb_hook = WasbHook(self.azure_conn_id)
        
        self.log.info(wasb_hook.get_conn)
        self.log.info("Created Azure Connections")

        riot_hook = riotHook(self.riot_conn_id)

        if self.data_url == 'champions':
            response_data = riot_hook.get_champions(self.version)
        elif self.data_url == 'maps':
            response_data = riot_hook.get_maps(self.version)
        elif self.data_url == 'items':
            response_data = riot_hook.get_items(self.version)
        elif self.data_url == 'runesreforged':
            response_data = riot_hook.get_runes_reforged(self.version)
        elif self.data_url == 'summonerspells':
            response_data = riot_hook.get_summoner_spells(self.version)

        # Write tweet data to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "reponse_data.json")
            with open(tmp_path, 'w') as fp:
                json.dump(response_data, fp)

            # Upload file to Azure Blob.
            self.log.info("About to load to Azure!")
            self.log.info(self.data_url)
            wasb_hook.load_file(
                tmp_path,
                container_name="datadragon-{url_path}".format(url_path = self.data_url),
                blob_name="{year}/{month}/{day}/{hour}.json".format(
                    year=self.end_epoch.year,
                    month=self.end_epoch.month,
                    day=self.end_epoch.day,
                    hour=self.end_epoch.hour)
                )
