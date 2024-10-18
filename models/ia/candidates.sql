with raw_candidates as (
    select
        c.Id,
        SUBSTRING(c.Name, 1, CHARINDEX(' ', c.Name) - 1) as FirstName,
        SUBSTRING(c.Name, CHARINDEX(' ', c.Name) + 1, LEN(c.Name)) as LastName,
        c.Email,
        c.PhoneNumber,
        r.ReviewDescription,
        o.Address as OfficeAddress
    from {{ source('internal_app', 'candidates') }} as c  
    left join {{ source('internal_app', 'offices') }} as o 
        on c.OfficeId = o.Id
    left join {{ ref("reviewed") }} as r
        on c.IsReviewed = r.ReviewStatus
) 

select *
from raw_candidates;
