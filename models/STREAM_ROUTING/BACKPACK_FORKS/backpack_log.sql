{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='BACKPACK',
                        alias='BACKPACK_LOG',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('backpack_stream')}} 
                     where "TABLE" = 'backpack_log' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1