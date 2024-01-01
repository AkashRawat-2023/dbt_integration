{{
    config(
        materialized = 'view'
    )
}}
WITH A AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
),
L AS(
    SELECT * FROM {{ref("src_learning_cleansed")}}
)

SELECT
*
FROM A INNER JOIN L ON A.FTV_ID = L.EMPLOYEE_NUMBER
WHERE is_deleted <> 'True'