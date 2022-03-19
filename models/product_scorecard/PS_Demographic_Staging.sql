{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='PRODUCT_SCORECARD',
                        alias='PS_DEMOGRAPGHIC_STAGING',
                        materialized='table',
                      )
                    }}

SELECT  b.*
FROM
{{ref('demographic')}} a join DEMOGRAPHIC_STREAM b on a.uuid = b.uuid