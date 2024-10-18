with raw_skills as (
    select 
        s.Id,
        REPLACE(LOWER(s.Name), ' ', '') as skill,
        c.FirstName,
        c.LastName,
        s.CandidateId
    from {{ source('internal_app', 'skills') }} as s 
    left join candidates as c 
        on s.CandidateId = c.Id
)

select * 
from raw_skills;