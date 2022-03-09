{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='BACKPACK',
                        alias='PUBLIC_BACKPACK',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('backpack_stream')}} 
                     where "TABLE" = 'public_backpack' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1