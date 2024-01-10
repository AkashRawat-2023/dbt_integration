{{ config(
    materialized='incremental',
    unique_key='enrollment_id',
    merge_exclude_columns=['inserted_at'],
    full_refresh=false,

) }}

SELECT * FROM {{ref('src_learning_cleansed')}}
