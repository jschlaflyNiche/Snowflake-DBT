{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='HTTP_PORTAL',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'http_portal' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1