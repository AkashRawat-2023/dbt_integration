-- Test to ensure that the "headcount" column in the fact table is always 1
select
  *
from
  {{ ref("fact_learning") }}
where
  headcount <> 1
