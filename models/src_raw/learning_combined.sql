{{
    config(
        materialized = 'incremental',
        unique_key= 'enrollment_id',
        merge_exclude_columns = ['inserted_at'],
        schema = 'RAW',
        full_refresh=false

    )
}}


SELECT * FROM {{ref('src_learning_cleansed')}}






-- {%if is_incremental() %}
-- where received_At > (select max(received_at) from {{this}})
-- {% endif %}