{{
config
    ({
        "materialized":'table',
        "schema":'GOLD'
    })
}}

WITH fact AS 
(
SELECT  
    fact.date_id,
    CAST (fact.dept_id AS INTEGER) as dept_id ,
    CAST (fact.store_id AS INTEGER) as store_id,
    fact.store_size,
    CAST(fact.store_weekly_sales AS FLOAT) as store_weekly_sales  ,
    CAST(fact.TEMPERATURE AS FLOAT) as TEMPERATURE,
	CAST(fact.FUEL_PRICE AS FLOAT) as FUEL_PRICE,
	fact.MARKDOWN1,
	fact.MARKDOWN2,
	fact.MARKDOWN3,
	fact.MARKDOWN4,
	fact.MARKDOWN5,
	CAST(fact.CPI AS FLOAT) as CPI,
	CAST(fact.UNEMPLOYMENT AS FLOAT) as UNEMPLOYMENT 
FROM {{ref('transformed_fact_table')}} fact 
WHERE fact.DBT_VALID_TO is NULL
),
walmart_store_dim as (
SELECT 
    store_dept_id,
    store_id,
    dept_id
FROM {{ref('transformed_store_dim')}} fact
)
SELECT 
    s.store_dept_id,
    f.* 
FROM fact f 
INNER JOIN walmart_store_dim s ON f.dept_id=s.dept_id AND f.store_id=s.store_id
