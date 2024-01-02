{{
    config(
        materialized = 'incremental',
        unique_key=['ENROLLMENT_ID', 'RECEIVED_AT'],
        schema = 'RAW'
    )
}}

SELECT * FROM {{ref('src_learning_cleansed')}}


-- {%if is_incremental() %}
-- where received_At > (select max(received_at) from {{this}})
-- {% endif %}