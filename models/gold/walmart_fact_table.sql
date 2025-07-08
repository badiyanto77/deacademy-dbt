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
    -- CAST(CONCAT(store_id,dept_id) as INTEGER) as store_dept_id,
    fact.date_id,
    CAST (fact.dept_id AS INTEGER) as dept_id ,
    CAST (fact.store_id AS INTEGER) as store_id,
    fact.store_size,
    CAST(fact.store_weekly_sales AS FLOAT) as store_weekly_sales  ,
    CAST(fact.TEMPERATURE AS FLOAT) as TEMPERATURE,
	CAST(fact.FUEL_PRICE AS FLOAT) as FUEL_PRICE,
	fact.MARKDOWN1,
	fact.MARKDOWN2 ,
	fact.MARKDOWN3 ,
	fact.MARKDOWN4 ,
	fact.MARKDOWN5 ,
	CAST(fact.CPI AS FLOAT) as CPI,
	CAST(fact.UNEMPLOYMENT AS FLOAT) as UNEMPLOYMENT 
FROM {{source('walmart_fact_table','TRANSFORMED_FACT_TABLE')}} fact 
),
walmart_store_dim as (
SELECT 
    store_dept_id,
    store_id,
    dept_id
FROM {{source('walmart_store_dim','TRANSFORMED_STORE_DIM')}} fact
)
SELECT 
    s.store_dept_id,
    f.* 
FROM fact f 
INNER JOIN walmart_store_dim s ON f.dept_id=s.dept_id AND f.store_id=s.store_id
