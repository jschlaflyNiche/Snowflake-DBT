{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='LEAD_CUSTOM_PROPERTY',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'lead_custom_property' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1