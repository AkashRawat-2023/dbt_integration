{%- set table_to_truncate = ref('learning_combined') -%}

TRUNCATE TABLE {{ table_to_truncate }};
