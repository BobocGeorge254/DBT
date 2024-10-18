{{ config(materialized='table') }}

with human_data as (
    select 1 as id, 'John' as first_name, 'Doe' as last_name, CAST('1985-07-10' AS DATE) as birth_date, 'john.doe@example.com' as email
    union all
    select 2 as id, 'Jane' as first_name, 'Smith' as last_name, CAST('1947-12-10' AS DATE) as birth_date, 'jane.smith@example.com' as email
    union all
    select 3 as id, 'Robert' as first_name, 'Johnson' as last_name, CAST('1989-05-01' AS DATE) as birth_date, 'robert.j@example.com' as email
)

select *
from human_data
where {{ validate_age('birth_date') }}
