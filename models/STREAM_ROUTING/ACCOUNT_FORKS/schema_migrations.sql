{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='ACCOUNT',
                        alias='SCHEMA_MIGRATIONS',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('account_stream')}} 
                     where "TABLE" = 'schema_migrations' qualify row_number() over (partition by data:version order by (data:__source_ts_ms) desc nulls last) = 1