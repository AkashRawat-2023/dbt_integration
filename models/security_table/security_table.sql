{{
    config(
        materialized = 'table'
    )
}}

select p.email_address as user_id, s.opco from {{ref('src_active_employees_cleansed')}} p
inner join {{ref('dim_segment')}} s
on p.department_name = s.department_name 
    and p.sub_opco = s.sub_opco
    and p.opco = s.opco
    and p.segment = s.segment
 where user_id is not null