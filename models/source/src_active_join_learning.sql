{{
    config(
        materialized = 'table'
    )
}}

WITH max_received_At AS (
    SELECT max(RECEIVED_AT) AS max_received
    FROM {{ref('learning_combined')}}
),
A AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
),
L AS(
    SELECT * FROM {{ref("learning_combined")}}
    WHERE RECEIVED_AT = (SELECT max_received FROM max_received_At)
)

SELECT
*
FROM A INNER JOIN L ON A.FTV_ID = L.EMPLOYEE_NUMBER
WHERE is_deleted <> 'True'
