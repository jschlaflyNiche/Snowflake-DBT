{{
  config(
    database='DEV_PG_RDS_REPLICATION',
    schema='ACCOUNT',
    alias='ACCOUNT_STREAM',
    materialized='table',
  )
}}


SELECT * FROM {{ref('snowpipe_change_staging')}} where "DATABASE" = 'account'