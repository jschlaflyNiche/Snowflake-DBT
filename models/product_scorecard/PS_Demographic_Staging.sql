{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='PRODUCT_SCORECARD',
                        alias='PS_DEMOGRAPGHIC_STAGING',
                        materialized='table',
                      )
                    }}

SELECT  *
FROM
{{ref('demographic')}} a join ACCOUNT_STREAM b on a.uuid = b.uuid