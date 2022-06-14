{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='match_id'
    )
}}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE match_id > (SELECT max(match_id) from {{ this }})

{% endif %}