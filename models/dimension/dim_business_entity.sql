{{
    config(
        materialized = 'incremental',
        unique_key = ['business_unit_name','legal_entity_name']
    )
}}
WITH business AS (
    SELECT
    distinct
    business_unit_name,legal_entity_name
    FROM
    {{ref('src_active_join_learning')}}
)
SELECT *, {{dbt_utils.default__generate_surrogate_key(['business_unit_name','legal_entity_name'])}} AS entity_id
FROM business
-- SELECT *,ROW_NUMBER() OVER(ORDER BY business_unit_name,legal_entity_name) AS entity_id
--  FROM business