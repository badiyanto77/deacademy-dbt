{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('STORE_SRC'),
        "schema":'BRONZE'
    })
}}


with stores as 
(
    SELECT 
        Store,
        Type,
        Size
    FROM {{source('store','STORE_SRC')}}
    
)
select * from stores 