{{
config
    ({
        "materialized":'incremental',
        "pre_hook" : copy_csv('DEPARTMENT'),
        "pre_hook" : copy_csv('STORES'),
        "pre_hook" : copy_csv('FACT'),
        "schema":'SILVER',
        "incremental_strategy" : 'merge',
        "unique_key" : 'Date',
        "merge_exclude_columns" : ['INSERT_DTS']
    })
}}


WITH UpdatedTransformedDateDim AS 
(
    SELECT 
        DISTINCT
        CAST(CONCAT(SUBSTRING(d.Date,1,4),SUBSTRING(d.Date,6,2),SUBSTRING(d.Date,9,2)) AS INTEGER) AS Date_Id,
        CAST(d.Date AS DATE) AS Date,
        CAST(d.IsHoliday AS BOOLEAN) as IsHoliday,
        CreatedAt
    FROM {{source('department','DEPARTMENT')}} d 
    ORDER BY Date ASC 
),
ExistingTransformedDateDim AS 
(
    SELECT * FROM {{this}}
),
ProposedTransformedDateDim AS 
(
    SELECT 
        a.Date_ID,
        a.Date,
        a.IsHoliday,
        b.IsHoliday AS OriginalIsHoliday,
        a.CreatedAt,
        CURRENT_TIMESTAMP AS INSERT_DTS,
        CURRENT_TIMESTAMP AS UPDATE_DTS
    FROM UpdatedTransformedDateDim a 
    LEFT JOIN ExistingTransformedDateDim b ON a.Date_ID=b.Date_ID
    {% if is_incremental() %}
    WHERE 
        a.CreatedAt > (SELECT max(INSERT_DTS) FROM {{this}}) AND  
        (a.IsHoliday <> b.IsHoliday OR b.Date_ID IS NULL)
    {% endif%}
)
SELECT * FROM ProposedTransformedDateDim 


