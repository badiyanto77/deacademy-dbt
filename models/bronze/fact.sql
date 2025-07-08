{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('FACT_SRC'),
        "schema":'BRONZE'
    })
}}


with fact as 
(
    SELECT 
        Store,
        Date,
        Temperature,
        Fuel_Price,
        MarkDown1,
        MarkDown2,
        MarkDown3,
        MarkDown4,
        MarkDown5,
        CPI,
        Unemployment,
        IsHoliday
    FROM  {{source('fact','FACT_SRC')}}
)
select * from fact