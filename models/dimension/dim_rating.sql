{{
    config(
        materialized = 'table'
    )
}}
WITH rating AS (
    SELECT  potential_rating,performance_rating
FROM {{ref('src_active_join_learning')}} 
WHERE performance_rating is not null and potential_rating is not null 
GROUP BY potential_rating,performance_rating
ORDER BY  potential_rating,performance_rating
)

SELECT *, {{dbt_utils.generate_surrogate_key(['performance_rating', 'potential_rating'])}} AS rating_id 
FROM rating

-- SELECT *, ROW_NUMBER() OVER (ORDER BY potential_rating,performance_rating) AS rating_id 
-- FROM rating