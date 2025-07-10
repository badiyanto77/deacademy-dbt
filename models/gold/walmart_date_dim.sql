{{
config
    ({
        "materialized":'table',
        "schema":'GOLD'
    })
}}

WITH Date AS 
(
SELECT 
    
    CAST(date_id AS INTEGER) AS date_id,
    Date,
    IsHoliday 
FROM {{ref('transformed_date_dim')}}
ORDER BY Date ASC 
)
SELECT * FROM Date 

