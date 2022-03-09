{{
  config(
    database='DEV_SNOWPIPE_DB',
    schema='PUBLIC',
    materialized='table',
  )
}}


SELECT "DATABASE", "TABLE", DATA FROM STREAM_STAGING_S3