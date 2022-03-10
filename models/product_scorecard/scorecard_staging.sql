{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='PUBLIC',
                        alias='SCORECARD_STAGING',
                        materialized='table_custom',
                      )
                    }}

SELECT  "DATABASE",
        "TABLE",
        DATA
         
FROM
{{ref('account')}} a join "DEV_PG_RDS_REPLICATION"."PUBLIC"."ACCOUNT_STREAM" b on a.uuid = b.uuid