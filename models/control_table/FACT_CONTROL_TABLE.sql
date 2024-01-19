{{
    config(
        materialized = 'incremental',
        unique_key = 'TABLE_NAME',
        full_refresh = false
    )
}}

SELECT max(updated_at) as LAST_UPDATED_DATE, min(updated_at) as INITIAL_UPDATED_DATE,
'fact_learning' as TABLE_NAME
FROM AKASH.DEV.FACT_LEARNING