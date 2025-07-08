{{
config
    ({
        "materialized":'table',
        "pre_hook" : copy_csv('DEPARTMENT_SRC'),
        "schema":'BRONZE'
    })
}}


with department 
as 
(
    SELECT 
        Store,
        Dept,
        Date,
        WeeklySales,
        IsHoliday
    FROM {{source('department','DEPARTMENT_SRC')}}
)
select * from department
