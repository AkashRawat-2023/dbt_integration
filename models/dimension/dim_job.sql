{{
    config(
        materialized = 'incremental',
        unique_key = ['JOB_TITLE', 'JOB_FUNCTION','JOB_FAMILY','EEO_CATEGORY','CAREER_BAND','CAREER_LEVEL']
    )
}}

WITH dim_jobs AS(
    SELECT distinct  JOB_TITLE,JOB_FUNCTION,JOB_FAMILY,EEO_CATEGORY,CAREER_BAND,CAREER_LEVEL
    FROM {{ref('src_active_employees_cleansed')}}
)
SELECT *,
{{dbt_utils.generate_surrogate_key(['JOB_TITLE', 'JOB_FUNCTION','JOB_FAMILY','EEO_CATEGORY','CAREER_BAND','CAREER_LEVEL'])}} AS JOB_ID
FROM dim_jobs

-- SELECT *,ROW_NUMBER() OVER (ORDER BY JOB_TITLE,JOB_FUNCTION,JOB_FAMILY,EEO_CATEGORY,CAREER_BAND,CAREER_LEVEL) JOB_ID
--  FROM dim_jobs