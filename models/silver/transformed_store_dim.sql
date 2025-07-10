{{
config
    ({
        "materialized":'incremental',
        "schema":'SILVER',
        "incremental_strategy" : 'merge',
        "unique_key" : ['store_id', 'dept_id','store_dept_id'],
        "merge_exclude_columns" : ['INSERT_DTS']

    })
}}


WITH ExistingStoreDim AS 
(
    SELECT 
        store_dept_id,
        store_type,
        store_size    
    FROM {{this}} 
),
Department AS 
(
    SELECT
        DISTINCT
        Store,
        Dept 
    FROM  {{source('department','DEPARTMENT')}} 
),
UpdatedStoreDim as 
(
    SELECT 
         CONCAT('S',s.store,'D',d.dept) as store_dept_id,
         CAST(s.store as INTEGER) as store_id,
         CAST(d.dept as INTEGER) as dept_id,
         CAST(s.Type AS CHAR) as store_type,
         CAST(s.Size as INTEGER) as store_size,
         s.CreatedAt
    FROM department d 
    INNER JOIN {{source('stores','STORES')}}  s ON d.store = s.store 
    ORDER BY s.store ASC, d.dept ASC  
),
ProposedStoreDim AS 
(
    SELECT 
         a.store_dept_id,
         a.store_id,
         a.dept_id,
         a.store_type,
         a.store_size,
         a.CreatedAt,
         CURRENT_TIMESTAMP AS INSERT_DTS,
         CURRENT_TIMESTAMP AS UPDATE_DTS
    FROM UpdatedStoreDim a 
    LEFT JOIN ExistingStoreDim b ON a.store_dept_id=b.store_dept_id
    {% if is_incremental() %}
    WHERE 
        a.CreatedAt > (SELECT max(INSERT_DTS) FROM {{this}}) AND  
        (a.store_type <> b.store_type OR a.store_size <> b.store_size OR b.store_dept_id IS NULL) 
    {% endif%}
)
SELECT * FROM ProposedStoreDim 

