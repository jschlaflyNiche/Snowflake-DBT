{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='FAILED_TRANSFORMATION_LOG',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'failed_transformation_log' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1