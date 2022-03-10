{{
  config(
    database='DEV_PG_RDS_REPLICATION',
    schema='PRODUCT_SCORCARD',
    alias='LITE_REG',
    materialized='table',
  )
}}

select count(*) from {{ref('scorecard_staging')}}
where email is not null
and first is null 
and last is null 
and birth is null 