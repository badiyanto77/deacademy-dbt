{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('DEPARTMENT'),
        "schema":'SILVER'
    })
}}

WITH Date AS 
(
SELECT 
    DISTINCT
    CAST(CONCAT(SUBSTRING(Date,1,4),SUBSTRING(Date,6,2),SUBSTRING(Date,9,2)) AS INTEGER) AS Date_Id,
    CAST(Date AS DATE) AS Date,
    CAST(IsHoliday AS BOOLEAN) as IsHoliday 
FROM {{source('department','DEPARTMENT')}}
ORDER BY Date ASC 
)
SELECT * FROM Date 

