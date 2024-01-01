
WITH src_learning AS(
    SELECT * FROM {{source('AKASH','LEARNING')}}
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
DAYS_UNTIL_DUE,
EMPLOYEE_NUMBER,
NVL(ENROLLMENT_METHOD,'NA') AS ENROLLMENT_METHOD,
NVL(IS_ENROLLED,'NA') AS IS_ENROLLED,
LANGUAGE,
LAST_LOGGEDIN as LAST_LOGGED_IN,
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
NVL(is_deleted,'NA') as is_deleted,
NVL(is_active,'NA') as is_active
FROM src_learning
 
