{{ config(
    materialized='ephemeral',
    full_refresh = false
) }}

WITH src_learning AS(
    SELECT * FROM {{source('AKASH','LEARNING_INCREMENTAL')}}
)
SELECT
ENROLLMENT_ID,
NVL(ATTAINED_CERTIFICATE,'NA') AS ATTAINED_CERTIFICATE,
CERTIFICATE_DATE,
COURSE,
COURSE_NAME,
COURSE_TYPE,
NVL(COURSE_VERSION, 'NA') AS COURSE_VERSION,
DATE_COMPLETED,
DATE_EDITED, 
DATE_ENROLLED,
DATE_EXPIRES,
DATE_HIRED,
DATE_STARTED,
EMPLOYEE_NUMBER,
NVL(ENROLLMENT_METHOD,'NA') AS ENROLLMENT_METHOD,
NVL(IS_ENROLLED,'NA') AS IS_ENROLLED,
LANGUAGE,
PROGRESS,
SCORE,
CASE
    WHEN STATUS = 'NotComplete' THEN 'Not Complete'
    WHEN STATUS = 'NotStarted' THEN 'Not Started'
    WHEN STATUS = 'InProgress' THEN 'In Progress'
    WHEN STATUS = 'PendingApproval' THEN 'Pending Approval'
    WHEN STATUS = 'PendingEvaluationRequired' THEN 'Pending Evaluation Required'
    WHEN STATUS = 'NotApplicable' THEN 'Not Applicable'
    WHEN STATUS = 'N/A' THEN 'Not Applicable'
    ELSE STATUS
  END AS STATUS
,
TIMESPENT_MIN,
NVL(VENDOR, 'NA') AS
VENDOR,
CASE
    WHEN is_deleted = 'TRUE' THEN 'True'
    WHEN is_deleted = 'FALSE' THEN 'False'
    WHEN is_deleted IS NULL THEN 'NA'
    ELSE is_deleted
  END AS is_deleted,
NVL(is_active,'NA') as is_active,
CURRENT_TIMESTAMP() AS inserted_at,
CURRENT_TIMESTAMP() AS updated_at
FROM src_learning




-- DAYS_UNTIL_DUE,