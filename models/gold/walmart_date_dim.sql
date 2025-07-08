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
FROM {{source('walmart_date_dim','TRANSFORMED_DATE_DIM')}}
ORDER BY Date ASC 
)
SELECT * FROM Date 

