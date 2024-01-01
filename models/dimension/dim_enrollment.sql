{{
    config(
        materialized = 'table'
    )
}}
WITH enroll AS (
    SELECT distinct ATTAINED_CERTIFICATE,ENROLLMENT_METHOD,IS_ENROLLED, is_deleted, is_active
    FROM {{ref('src_active_join_learning')}}
)

SELECT *,{{dbt_utils.generate_surrogate_key(['ATTAINED_CERTIFICATE', 'ENROLLMENT_METHOD','IS_ENROLLED','is_deleted','is_active'])}} AS ROW_ID
FROM enroll

-- SELECT *,ROW_NUMBER() OVER(ORDER BY ATTAINED_CERTIFICATE, ENROLLMENT_METHOD, IS_ENROLLED, is_deleted, is_active) AS ROW_ID
-- FROM enroll