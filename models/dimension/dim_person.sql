{{
    config(
        materialized = 'incremental',
        unique_key = 'EMPLOYEE_NUMBER'
    )
}}

WITH persons AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
)

SELECT
distinct
FTV_ID as EMPLOYEE_NUMBER,
EMAIL_ADDRESS,
PERSON_DISPLAY_NAME,
Person_Type,
Worker_Type,
Age,
L1_Leader,
Date_of_Birth
FROM
persons