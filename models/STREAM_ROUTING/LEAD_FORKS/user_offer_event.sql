{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='USER_OFFER_EVENT',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('lead_stream')}} 
                     where "TABLE" = 'user_offer_event' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1