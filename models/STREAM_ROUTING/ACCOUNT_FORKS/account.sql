{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='ACCOUNT',
                        alias='ACCOUNT',
                        materialized='table_custom',
                      )
                    }}

SELECT  "DATABASE",
        "TABLE",
        DATA
         
FROM
{{ref('account_stream')}} WHERE  "TABLE" = 'account' 
qualify row_number() over (partition BY data:uuid ORDER BY (data:__source_ts_ms) DESC nulls last) = 1

