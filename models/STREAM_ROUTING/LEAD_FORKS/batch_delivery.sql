{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='BATCH_DELIVERY',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'batch_delivery' qualify row_number() over (partition by data:batch_uuid,data:delivery_log_uuid order by (data:__source_ts_ms) desc nulls last) = 1