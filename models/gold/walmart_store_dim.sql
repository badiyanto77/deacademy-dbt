{{
config
    ({
        "materialized":'table',
        "schema":'GOLD'
    })
}}


with store_dim as 
(
    SELECT 
         CAST(store_dept_id as VARCHAR) as store_dept_id,
         CAST(store_id as INTEGER) as store_id,
         CAST(dept_id as INTEGER) as dept_id,
         CAST(store_type AS CHAR) as store_type,
         CAST(store_size as INTEGER) as store_size
    FROM {{source('walmart_store_dim','TRANSFORMED_STORE_DIM')}}
)
select * from store_dim

