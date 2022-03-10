{{
  config(
    database='DEV_PG_RDS_REPLICATION',
    schema='PRODUCT_SCORECARD',
    alias='ACCOUNT',
    materialized='table_stream',
  )
}}


SELECT * FROM temp