WITH last_run AS (
    SELECT max(updated_at) AS last_run_date
    FROM {{ref('learning_combined')}}
),
A AS (
    SELECT * FROM {{ref("src_active_employees_cleansed")}}
),
L AS(
    SELECT * FROM {{ref("learning_combined")}}
    WHERE updated_at = (SELECT last_run_date FROM last_run)
)

SELECT
*
FROM A INNER JOIN L ON A.FTV_ID = L.EMPLOYEE_NUMBER