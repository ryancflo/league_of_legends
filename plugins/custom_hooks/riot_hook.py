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
    
    def get_matchlist_by_puuid(   
        self,
        my_region: str,
        puuid: str,
        start: int = None,
        count: int = None,
        queue: int = None,
        queue_type: str = None,
        start_epoch: int = None,
        end_epoch: int = None
    ):
        #/lol/match/v5/matches/by-puuid/{puuid}/ids
        #500 requests every 10 seconds
        match_history_list = self.lol_watcher.match.matchlist_by_puuid(region=my_region, puuid=puuid, count=count, type=queue_type, start_time=start_epoch, end_time=end_epoch)
        return match_history_list

    def get_match_byid(self, my_region: str, match_id: str):
        #/lol/match/v5/matches/{matchId}
        #250 requests every 10 seconds
        match_details = self.lol_watcher.match.by_id(my_region, match_id)
        return match_details

    def get_leagues(self, my_region: str, queue: str, tier: str, division: str, page: int):
        #/lol/league-exp/v4/entries/{queue}/{tier}/{division}
        #50 requests every 10 seconds
        summoners_league = self.lol_watcher.league.entries(my_region, queue, tier, division, page)
        return summoners_league

    def get_summoner_byid(self, my_region: str, summoner_id: str):
        #/lol/summoner/v4/summoners/{encryptedSummonerId}
        #1600 requests every 1 minutes
        summoner = self.lol_watcher.summoner.by_id(my_region,summoner_id)
        return summoner

    #DataDragonAPI-champions
    def get_champions(self, league_version: str):

        champions = self.lol_watcher.data_dragon.champions(league_version)
        return champions['data']

    #DataDragonAPI-maps
    def get_maps(self, league_version: str):

        maps = self.lol_watcher.data_dragon.maps(league_version)
        return maps['data']

    #DataDragonAPI-items
    def get_items(self, league_version: str):

        items = self.lol_watcher.data_dragon.items(league_version)
        return items

    #DataDragonAPI-runes_reforged
    def get_runes_reforged(self, league_version: str):

        runes_reforged = self.lol_watcher.data_dragon.runes_reforged(league_version)
        return runes_reforged
        
    #DataDragonAPI-summoner_spells
    def get_summoner_spells(self, league_version: str):

        summoner_spells = self.lol_watcher.data_dragon.summoner_spells(league_version)
        return summoner_spells['data']
    
    def get_challenger_players(self, my_region: str, mode: str):

        challengers = self.lol_watcher.league.challenger_by_queue(my_region, mode)
        return challengers

    def get_summoner_byname(self, my_region: str, summoner_id: str):

        summoner = self.lol_watcher.summoner.by_name(my_region,'dild0wacker')
        return summoner