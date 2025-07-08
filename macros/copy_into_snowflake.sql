{% macro copy_csv(table_name) %}
    {% if table_name=='DEPARTMENT' %}
        {% set sql %}
        --Delete the data from the copy table before running the copy command
        delete from {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }};
        --Copy the data from the snowflake external stage to snowflake table
        COPY INTO {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }}
        FROM
        (
            SELECT 
                $1 Store,
                $2 Dept,
                $3 Date,
                $4 WeeklySales,
                $5 IsHoliday
            FROM  @{{ var('stage_name') }}
            (
                FILE_FORMAT => 'MY_CSV_FORMAT', -- Replace with your file format name
                PATTERN => '.*department\\.csv' -- Pattern to match the specific file name
            )
        )
        FORCE = TRUE;
        {% endset %}
        {{ return(sql) }}
   

    {% elif table_name=='STORES' %}
        {% set sql %}
        --Delete the data from the copy table before running the copy command
        delete from {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }};
        --Copy the data from the snowflake external stage to snowflake table
        COPY INTO {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }}
        FROM
        (
            SELECT 
                 $1 Store,
                 $2 Type,
                 $3 Size
            FROM  @{{ var('stage_name') }}
            (
                FILE_FORMAT => 'MY_CSV_FORMAT', -- Replace with your file format name
                PATTERN => '.*stores\\.csv' -- Pattern to match the specific file name
            )
        )
        FORCE = TRUE;
        {% endset %}
        {{ return(sql) }}
    
    {% elif table_name=='FACT' %}
        {% set sql %}
        --Delete the data from the copy table before running the copy command
        delete from {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }};
        --Copy the data from the snowflake external stage to snowflake table
        COPY INTO {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_name }}
        FROM
        (
            SELECT 
                $1 Store,
                $2 Date,
                $3 Temperature,
                $4 Fuel_Price,
                $5 MarkDown1,
                $6 MarkDown2,
                $7 MarkDown3,
                $8 MarkDown4,
                $9 MarkDown5,
                $10 CPI,
                $11 Unemployment,
                $12 IsHoliday
                FROM  @{{ var('stage_name') }}
                (
                    FILE_FORMAT => 'MY_CSV_FORMAT', -- Replace with your file format name
                    PATTERN => '.*fact\\.csv' -- Pattern to match the specific file name
                )
        )
        FORCE = TRUE;
        {% endset %}
        {{ return(sql) }}

    {% else %}
            {{ return('') }}
    {% endif %}
{% endmacro %}