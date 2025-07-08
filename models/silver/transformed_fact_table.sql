{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('FACT'),
        "schema":'SILVER'
    })
}}

WITH fact AS 
(
SELECT  
    date_dim.date_id,
    dept.dept AS dept_id,
    dept.store AS store_id,
    store_dim.store_size,
    dept.weeklysales AS store_weekly_sales,
    fact.TEMPERATURE,
	fact.FUEL_PRICE,
	fact.MARKDOWN1,
	fact.MARKDOWN2 ,
	fact.MARKDOWN3 ,
	fact.MARKDOWN4 ,
	fact.MARKDOWN5 ,
	fact.CPI,
	fact.UNEMPLOYMENT 
FROM {{source('department','DEPARTMENT')}} dept 
INNER JOIN {{source('date_dim','TRANSFORMED_DATE_DIM')}} date_dim ON date_dim.Date= dept.date
INNER JOIN {{source('store_dim','TRANSFORMED_STORE_DIM')}} store_dim  ON store_dim.store_id= dept.store AND store_dim.dept_id= dept.dept 
INNER JOIN {{source('fact','FACT')}} fact ON fact.store=dept.store AND fact.date=dept.date
)
SELECT * FROM fact 
