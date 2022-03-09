{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='ACCOUNT',
                        alias='ACTION_FIELD_GROUP',
                        materialized='table_custom',
                      )
                    }}
SELECT "DATABASE", "TABLE", DATA
                    FROM {{ref('account_stream')}} 
                     where "TABLE" = 'action_field_group' qualify row_number() over (partition by data:id order by (data:__source_ts_ms) desc nulls last) = 1