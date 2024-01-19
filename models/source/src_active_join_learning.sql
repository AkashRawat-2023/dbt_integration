{{
    config(
        materialized = 'incremental',
        pre_hook="TRUNCATE TABLE DEV.src_active_join_learning;",
    )
}}

WITH last_run AS (
    SELECT LAST_UPDATED_DATE FROM {{ref('FACT_CONTROL_TABLE')}} WHERE TABLE_NAME = 'fact_learning'
),
A AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
),
L AS (
    SELECT * 
    FROM {{ref("learning_combined")}}
    {% if is_incremental() %}
    WHERE updated_at > (SELECT LAST_UPDATED_DATE FROM last_run) 
    {% endif %}

)

SELECT
*
FROM A INNER JOIN L ON A.FTV_ID = L.EMPLOYEE_NUMBER






-- when i rin dbt run --full refresh
-- WHERE updated_at > (SELECT INITIAL_UPDATED_DATE - INTERVAL '1 day' FROM last_run)