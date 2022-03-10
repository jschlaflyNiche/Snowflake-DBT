{{
  config(
    database='DEV_SNOWPIPE_DB',
    schema='PUBLIC',
    materialized='table',
  )
}}


SELECT * FROM "DEV_PG_RDS_REPLICATION"."POSTGRES_STREAMS".PG_KAFKA_STREAM