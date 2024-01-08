{{
    config(
        materialized = 'view'
    )
}}

WITH A AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
),
L AS (
    SELECT * FROM {{ref("learning_combined")}}
)

SELECT
    *
FROM A INNER JOIN L ON A.FTV_ID = L.EMPLOYEE_NUMBER
