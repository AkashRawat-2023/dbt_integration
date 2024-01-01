{{
    config(
        materialized = 'table'
    )
}}
WITH RECURSIVE generated_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1) as row_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1 as calender_date,
        DAYNAME(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1) as day_name,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'MMMM') as month_name,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'Mon') as month_name_abbr,
        CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/6) as cal_half,
        CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/3) as cal_qtr,
        EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) as cal_month,
        WEEK(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1) as cal_week,
        EXTRACT(YEAR FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) as cal_year,
        CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/4) as cal_trimester,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-2 as day_ago_dt,
        SEQ4()-1 as day_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-15 as half_ago_dt,
        CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/6) - 1 as half_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-31 as month_ago_dt,
        EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) - 1 as month_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-91 as quarter_ago_dt,
        EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) - 3 as quarter_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-121 as trimester_ago_dt,
        EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) - 4 as trimester_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-7 as week_ago_dt,
        TO_CHAR(WEEK(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-7)) as week_ago_wid,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-365 as year_ago_dt,
        EXTRACT(YEAR FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) - 1 as year_ago_wid,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'YYYY') as per_name_year,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'YYYY') || ' H' ||
            TO_CHAR(CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/6)) as per_name_half,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'YYYY') || ' Q' ||
            TO_CHAR(CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/3)) as per_name_qtr,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'YYYY') || ' T' ||
            TO_CHAR(CEIL(EXTRACT(MONTH FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))/4)) as per_name_ter,
        TO_CHAR(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1, 'YYYY-MM') as per_name_month,
        TO_CHAR(WEEK(TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1)) || '-' ||
            TO_CHAR(EXTRACT(YEAR FROM (TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1))) as per_name_week,
            CONCAT(TO_CHAR(calender_date, 'YYYY'), '-', TO_CHAR(calender_date, 'MM')) as month_year
    FROM (
        SELECT
            SEQ4() as SEQ4
        FROM TABLE(GENERATOR(ROWCOUNT => 55255))
    )
    WHERE TO_DATE('1900-01-01', 'YYYY-MM-DD') + SEQ4()-1 <= CURRENT_DATE()
)

-- Create the w_day_d table


-- Create the w_day_d table
SELECT
    EXTRACT(YEAR FROM calender_date) * 10000 +
    EXTRACT(MONTH FROM calender_date) * 100 +
    EXTRACT(DAY FROM calender_date) AS numeric_date,
    calender_date,
    day_name,
    month_name,
    month_name_abbr,
    cal_half,
    cal_qtr,
    cal_month,
    cal_week,
    cal_year,
    cal_trimester,
    day_ago_dt,
    day_ago_wid,
    half_ago_dt,
    half_ago_wid,
    month_ago_dt,
    month_ago_wid,
    quarter_ago_dt,
    quarter_ago_wid,
    trimester_ago_dt,
    trimester_ago_wid,
    week_ago_dt,
    week_ago_wid,
    year_ago_dt,
    year_ago_wid,
    per_name_year,
    per_name_half,
    per_name_qtr,
    per_name_ter,
    per_name_month,
    per_name_week
FROM
    generated_data
