{{
  config(
    database='DEV_PG_RDS_REPLICATION',
    schema='BACKPACK',
    alias='BACKPACK_STREAM',
    materialized='table',
  )
}}


SELECT * FROM {{ref('snowpipe_change_staging')}} where "DATABASE" = 'backpack'