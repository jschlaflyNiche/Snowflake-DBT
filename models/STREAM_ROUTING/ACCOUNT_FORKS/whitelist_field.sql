{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='ACCOUNT',
                        alias='WHITELIST_FIELD',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('account_stream')}} 
                     where "TABLE" = 'whitelist_field' qualify row_number() over (partition by data:name order by (data:__source_ts_ms) desc nulls last) = 1