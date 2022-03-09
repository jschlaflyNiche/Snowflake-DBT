{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='ACCOUNT',
                        alias='DEMOGRAPHIC',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('account_stream')}} 
                     where "TABLE" = 'demographic' qualify row_number() over (partition by data:uuid order by (data:__source_ts_ms) desc nulls last) = 1