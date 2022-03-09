{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='LEAD',
                        alias='PENDING_STAGE',
                        materialized='table',
                      )
                    }}

SELECT  "DATABASE",
        "TABLE",
        DATA
         
FROM
{{ref('lead_stream')}} WHERE  "TABLE" = 'pending' 
qualify row_number() over (partition BY data:client_id, data:item_uuid ORDER BY (data:__source_ts_ms) DESC nulls last) = 1

