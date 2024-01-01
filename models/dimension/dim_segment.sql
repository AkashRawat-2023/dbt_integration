{{
    config(
        materialized = 'table'
    )
}}


SELECT distinct
{{dbt_utils.generate_surrogate_key(['department_name', 'sub_opco','opco','segment'])}} AS segment_id,
department_name, sub_opco,opco,segment
FROM {{ref('src_active_employees_cleansed')}}

-- WITH dim_Segments AS(
--     SELECT distinct department_name, sub_opco,opco,segment
--     FROM {{ref('src_active_employees_cleansed')}}
-- )

-- SELECT *,ROW_NUMBER()OVER(ORDER BY department_name, sub_opco,opco,segment) AS segment_id
--  FROM dim_Segments

 