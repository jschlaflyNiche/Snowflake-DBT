{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='PRODUCT_SCORECARD',
                        alias='PS_ACCOUNT_STAGING',
                        materialized='table',
                      )
                    }}

SELECT  *
FROM
{{ref('account')}} a join ACCOUNT_STREAM b on a.uuid = b.uuid