{{
    config(
        materialized = 'table'
    )
}}

WITH cou AS(
    SELECT distinct COURSE,COURSE_NAME,COURSE_TYPE,COURSE_VERSION,LANGUAGE,VENDOR FROM {{ref('src_active_join_learning')}}
)
SELECT *,
{{dbt_utils.generate_surrogate_key(['COURSE', 'COURSE_NAME','COURSE_TYPE','COURSE_VERSION','LANGUAGE','VENDOR'])}} AS course_id
FROM cou