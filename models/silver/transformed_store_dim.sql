{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('STORES'),
        "schema":'SILVER'
    })
}}


with department as 
(
    SELECT
        DISTINCT
        Store,
        Dept 
    FROM {{source('department','DEPARTMENT')}}
),
store_dept as 
(
    SELECT 
         CONCAT('S',s.store,'D',d.dept) as store_dept_id,
         CAST(s.store as INTEGER) as store_id,
         CAST(d.dept as INTEGER) as dept_id,
         CAST(s.Type AS CHAR) as store_type,
         CAST(s.Size as INTEGER) as store_size
    FROM department d 
    INNER JOIN {{source('stores','STORES')}} s ON d.store = s.store 
    ORDER BY s.store ASC, d.dept ASC  
)
SELECT * FROM store_dept 
