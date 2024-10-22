{{ config(materialized='table') }}

with raw_candidates as (
    select
        c.Id,
        SUBSTRING(c.Name, 1, CHARINDEX(' ', c.Name) - 1) as FirstName,
        SUBSTRING(c.Name, CHARINDEX(' ', c.Name) + 1, LEN(c.Name)) as LastName,
        c.Email,
        c.PhoneNumber
    from {{ source('internal_app', 'candidates') }} as c  

) 

select *
from raw_candidates;