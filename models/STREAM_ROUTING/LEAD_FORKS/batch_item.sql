{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='BATCH_ITEM',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'batch_item' qualify row_number() over (partition by data:batch_uuid,data:item_uuid order by (data:__source_ts_ms) desc nulls last) = 1