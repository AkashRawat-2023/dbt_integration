{{config(
    materialized = 'incremental',
    unique_key = 'enrollment_id',
    merge_exclude_columns = ['inserted_at'],

    )
}}

WITH fact AS (SELECT *
FROM {{ref('src_active_join_learning')}}
 )

 SELECT 
 f.ENROLLMENT_ID,
 f.EMPLOYEE_NUMBER,
 f.Manager_FTV_ID,
 f.Salary,
 f.Currency_Code,
 f.Headcount,
 f.PROGRESS,
 f.SCORE,
 f.STATUS,
 f.TIMESPENT_MIN,
 f.FTE,
 f.Length_Of_Service,
 b.entity_id,
 r.rating_id,
 loc.location_id,
 j.job_id,
 seg.segment_id,
 asi.assig_id,
 c.course_id,
 en.ROW_ID,
 f.inserted_at,
 f.updated_at,
 NVL(TO_NUMBER(TO_CHAR(f.CERTIFICATE_DATE, 'YYYYMMDD')),-999) AS CERTIFICATE_DATE_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.DATE_COMPLETED, 'YYYYMMDD')),-999) AS DATE_COMPLETED_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.DATE_EDITED, 'YYYYMMDD')),-999) AS DATE_EDITED_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.DATE_ENROLLED, 'YYYYMMDD')),-999) AS DATE_ENROLLED_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.DATE_EXPIRES, 'YYYYMMDD')),-999) AS  DATE_EXPIRES_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.LAST_LOGGED_IN, 'YYYYMMDD')),-999) AS LAST_LOGGED_IN_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.Original_Hire_Date, 'YYYYMMDD')),-999) AS Original_Hire_Date_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.Latest_Hire_Date, 'YYYYMMDD')),-999) AS Latest_Hire_Date_KEY,
 NVL(TO_NUMBER(TO_CHAR(f.Adjusted_Service_Date, 'YYYYMMDD')),-999) AS Adjusted_Service_Date_KEY
 FROM fact f 
 LEFT OUTER JOIN {{ref("dim_rating")}} r 
                ON f.Performance_Rating=r.Performance_Rating 
                AND f.potential_rating=r.potential_rating
 LEFT OUTER JOIN {{ref('dim_business_entity')}} b 
                on f.business_unit_name = b.business_unit_name 
                AND  f.legal_entity_name = b.legal_entity_name
 LEFT OUTER JOIN {{ref('dim_work_location')}} loc
                on f.work_location = loc.location 
                and f.work_city = loc.city 
                and f.work_state = loc.state 
                and f.work_country = loc.country 
                and f.work_region = loc.region
 LEFT OUTER JOIN {{ref('dim_job')}} j
                on f.JOB_TITLE = j.JOB_TITLE 
                AND f.JOB_FUNCTION = j.JOB_FUNCTION 
                and f.JOB_FAMILY = j.JOB_FAMILY 
                and f.EEO_CATEGORY = j.EEO_CATEGORY 
                and f.CAREER_BAND = j.CAREER_BAND 
                and f.CAREER_LEVEL = j.CAREER_LEVEL
LEFT OUTER JOIN {{ref('dim_segment')}} seg
                on f.department_name = seg.department_name 
                and f.sub_opco = seg.sub_opco 
                and f.opco = seg.opco 
                and f.segment = seg.segment
LEFT OUTER JOIN {{ref('dim_assignment')}} asi
                on f.ASSIGNMENT_NAME = asi.ASSIGNMENT_NAME
                and f.ASSIGNMENT_EMPLOYMENT_CATEGORY = asi.ASSIGNMENT_EMPLOYMENT_CATEGORY
                and f.ASSIGNMENT_CURRENT_CHG_ACTION = asi.ASSIGNMENT_CURRENT_CHG_ACTION
                and f.ASSIGNMENT_CURRENT_CHG_REASON = asi.ASSIGNMENT_CURRENT_CHG_REASON
                and f.ASSIGNMENT_STATUS = asi.ASSIGNMENT_STATUS
                and f.HOURLY_SALARIED = asi.HOURLY_SALARIED
                and f.GRADE_NAME = asi.GRADE_NAME
                and f.Assignment_Current_Effective_Date = asi.Assignment_Current_Effective_Date
LEFT OUTER JOIN {{ref('dim_course')}} c
                on f.course_name = c.course_name
                and f.course = c.course
                and f.course_type = c.course_type
                and f.course_version = c.course_version
                and f.language = c.language
                and f.vendor = c.vendor
LEFT OUTER JOIN {{ref('dim_enrollment')}} en
                on f.ATTAINED_CERTIFICATE = en.ATTAINED_CERTIFICATE
                and f.ENROLLMENT_METHOD = en.ENROLLMENT_METHOD
                and f.IS_ENROLLED = en.IS_ENROLLED
                and f.is_deleted = en.is_deleted
                and f.is_active = en.is_active