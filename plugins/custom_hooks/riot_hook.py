from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.hooks.base_hook import BaseHook
from riotwatcher import LolWatcher, ApiError
import pandas as pd

class riotHook(BaseHook):
    def __init__(self, riot_conn_id='riot_conn_id'):
        self.connection = self.get_connection(riot_conn_id)
        self.api_key = self.connection.extra_dejson.get('API_KEY')
        self.lol_watcher = LolWatcher(self.api_key)
    
    def get_latest_match_history(   
        self,
        my_region: str,
        summoner_name: str,
        start: int = None,
        count: int = None,
        queue: int = None,
        type: str = None,
        start_epoch: int = None,
        end_epoch: int = None
    ):

        # A MatchHistory is a lazy list, meaning it's elements only get loaded as-needed.
        me = self.lol_watcher.summoner.by_name(my_region, summoner_name)
        match_history = self.lol_watcher.match.matchlist_by_puuid(my_region, me['puuid'], count)
        
        return match_history

    def get_match_byid(
        my_region: str, 
        match_type: str
    ):

        matches = self.lol_watcher.match.by_id(my_region, match_type)
        return matches

    #DataDragonAPI-champions
    def get_champions(version: str):

        champions = lol_watcher.data_dragon.champions(version=version)
        return champions

    #DataDragonAPI-maps
    def get_maps(version: str):

        maps = lol_watcher.data_dragon.maps(version=version)
        return maps

    #DataDragonAPI-items
    def get_items(version: str):

        items = lol_watcher.data_dragon.items(version=version)
        return items

    #DataDragonAPI-masteries
    # def get_masteries(version: str):

    #     masteries = lol_watcher.data_dragon.masteries(version=version)
    #     return masteries

    #DataDragonAPI-runes_reforged
    def get_runes_reforged(version: str):

        runes_reforged = lol_watcher.data_dragon.runes_reforged(version=version)
        return runes_reforged
        
    #DataDragonAPI-summoner_spells
    def get_summoner_spells(version: str):

        runes_reforged = lol_watcher.data_dragon.runes_reforged(version=version)
        return runes_reforged