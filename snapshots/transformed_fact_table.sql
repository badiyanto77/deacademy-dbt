{% snapshot transformed_fact_table %}

{{
config
    ({
        "strategy" : 'check',
         "unique_key" : ['store_id', 'dept_id','date_id'],
         "check_cols" : ['store_size','store_weekly_sales','TEMPERATURE','FUEL_PRICE','MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5','CPI','UNEMPLOYMENT'],
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
FROM {{source('dept_src','DEPARTMENT')}} dept 
INNER JOIN {{ref('transformed_date_dim')}} date_dim ON date_dim.Date= dept.date
INNER JOIN {{ref('transformed_store_dim')}} store_dim  ON store_dim.store_id= dept.store AND store_dim.dept_id= dept.dept 
INNER JOIN {{source('fact_src','FACT')}} fact ON fact.store=dept.store AND fact.date=dept.date
)
SELECT * FROM fact 

{% endsnapshot %}