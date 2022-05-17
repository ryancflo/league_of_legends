from airflow.plugins_manager import AirflowPlugin

from plugins.custom_hooks.riot_hook import riotHook

#Define the plugin class
class MyPlugins(AirflowPlugin):
    name = "my_plugin"

    hooks = [riotHook]