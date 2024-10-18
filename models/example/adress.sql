{{ config(materialized='table') }}

with address_data as (
    select 1 as id, 1 as human_id, '123 Main St' as street, 'New York' as city, 'NY' as state, '10001' as zip_code
    union all
    select 2 as id, 2 as human_id, '456 Elm St' as street, 'San Francisco' as city, 'CA' as state, '94105' as zip_code
    union all
    select 3 as id, 3 as human_id, '789 Oak St' as street, 'Chicago' as city, 'IL' as state, '60601' as zip_code
    union all
    select 4 as id, 2 as human_id, '987 Pine St' as street, 'Los Angeles' as city, 'CA' as state, '90001' as zip_code
    union all
    select 5 as id, 3 as human_id, '654 Maple St' as street, 'Miami' as city, 'FL' as state, '33101' as zip_code
)

select *
from address_data
where human_id is not null
