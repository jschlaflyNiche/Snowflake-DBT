{{
  config(
    database='DEV_PG_RDS_REPLICATION',
    schema='LEAD',
    alias='LEAD_STREAM',
    materialized='table',
  )
}}


SELECT * FROM {{ref('snowpipe_change_staging')}} where "DATABASE" = 'lead'