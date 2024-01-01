{{
    config(
        materialized = 'table'
    )
}}



WITH loc AS(
    SELECT distinct
    WORK_LOCATION, WORK_CITY, WORK_STATE, WORK_COUNTRY, WORK_REGION
    FROM {{ref('src_active_employees_cleansed')}}
)
SELECT 
{{dbt_utils.generate_surrogate_key(['WORK_LOCATION', 'WORK_CITY', 'WORK_STATE', 'WORK_COUNTRY', 'WORK_REGION'])}} AS location_id,
work_LOCATION as LOCATION, work_CITY AS CITY, work_STATE AS STATE, work_COUNTRY AS COUNTRY,
work_REGION AS REGION
FROM loc


-- SELECT *,ROW_NUMBER()OVER(ORDER BY LOCATION, CITY, STATE, COUNTRY, REGION) AS location_id
--  FROM loc