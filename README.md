# Data Engineering Project: League of Legends Pipeline


## Objective
The project consumes league of legends match data from Riot Games API of hundreds of players within a rank division. Match data consists of the details of a match such as its players, champions played, number of kills, and amount of damage a player did. The data is pulled periodically in a batch format hence this pipeline emulates a batch style job that applie extracts, transforms, creates tables on a schedule. The goal is to accumulate a big dataset for players to identify smurfs within their league division, generate player metrics and overall rank division metrics. The pipeline infrastructure is built using a modern data stack.

## Tools & Technologies

- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Transforms - [**DBT**](https://www.getdbt.com/)
- Orchestration & Batch Transformation - [**Airflow**](https://airflow.apache.org)
- Data Lake - [**Azure Data Lake**](https://azure.microsoft.com/en-us/solutions/data-lake/)
- Data Warehouse - [**Snowflake**](https://www.snowflake.com/)

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Architecture diagram](#architecture-diagram)
* [How it works](#how-it-works)
    * [Data Flow-Airflow Dags](#data-flow)
    * [Data Lineage-DBT](#data-lineage)
* [Dashboard](#dashboard)
    * [Dashboard](dashboard)
* [Setup](#setup)
    * [Airflow Connections](airflow-connections)
* [References](#references)
* [Work in Progress](#work-in-progress)

<!-- ARCHITECTURE DIAGRAM -->
## Architecture diagram

![Pipeline Architecture](https://github.com/ryancflo/league_of_legends/blob/master/images/LOL_arch.png)


<!-- DASHBOARD DIAGRAM -->
## Dashboard

#### Dashboard

![Dashboard](https://github.com/ryancflo/league_of_legends/blob/master/images/LOL_dashboard.PNG)

<!-- HOW IT WORKS -->
## How it works

### Data-Flow
##### Match Data DAG
 - Id: `matchData_dag`
 - Source Type: JSON API
 - Data Source: /lol/match/v5/matches/
    - Returns match details based on match id.

`staging_match_data`: Fetches match details, match info, and player data from the Riot API and loads it to Azure Blob Storage as a json file. Transformations and file type conversion is performed on match data.\
`azure_toSnowflake_paths-create_stage`: Create staging tables in Snowflake.\
`azure_toSnowflake_paths-azure_stage_snowflake`: Copy data from Azure Blob storage to raw staging tables.\
`data_quality`: A simple data quality check for nulls, emptiness, and row count.\
`dbt_run`: Runs 2 bash commands. dbt deps: Pulls the most recent version of the dependencies listed in your packages.yml and dbt run: executes compiled sql model files against the current target database).\
`dbt_test`: A simple data quality check for nulls and emptiness.\

![matchData DAG](https://github.com/ryancflo/league_of_legends/blob/master/images/match_dag.PNG)

##### Data Dragon DAG
 - Id: `dataDragon_dag`
 - Source Type: JSON API
 - Data Source: https://ddragon.leagueoflegends.com
    - Returns a json object of requested URL. Schedule should depend on League of legends patch updates.

`azure_toSnowflake_paths-stage_{}`: Fetches data dragon data from the RIOT API using the Riot watcher API wrapper. Maps, champions, masteries, runes, and items data is pulled and stored directly as json objects in Azure Blob Storage.
`azure_toSnowflake_paths-create_stage`: Creates staging tables for the respective static data it's map to in Snowflake.\
`azure_toSnowflake_paths-azure_stage_snowflake`: Copy data from Azure Blob storage to raw staging tables.\

![Data Dragon DAG](https://github.com/ryancflo/league_of_legends/blob/master/images/datadragondag.PNG)


### Data-Lineage
![data-lineage](https://github.com/ryancflo/league_of_legends/blob/master/images/lineage_graph.PNG)


<!-- SETUP -->
### Setup

### Airflow Connections

Setup your airflow connections 

| Service | Conn ID | Conn Type | Other fields |
| ------- | ------- | --------- | ------------------ |
| Snowflake | `snowflake_conn_id` | `Snowflake` | Fill out your Snowflake database credentials |
| Azure Blob Storage | `azure_conn_id` | `Azure Blob Storage` | Fill out your Azure container credentials |
| RIOT API | `riot_conn_id` | `HTTP` | Extras: {'API_KEY' : API_KEY} |

