{{ config(materialized='table') }}

WITH candidates_with_info AS (
    SELECT
        c.Id AS CandidateId,
        o.Address AS OfficeAddress,
        COUNT(ac.Id) AS NumberOfAssignees,
        cs.ToStatus as CandidateStatus,
        cs.TimeDifferenceInHours as TimeDifferenceInHours 
    FROM {{ source('internal_app', 'candidates') }} AS c  
    LEFT JOIN {{ source('internal_app', 'offices') }} AS o ON c.OfficeId = o.Id
    LEFT JOIN {{ source('internal_app', 'cards') }} AS ca ON ca.CandidateId = c.Id
    LEFT JOIN {{ source('internal_app', 'assigneecards') }} AS ac ON ac.CardId = ca.Id
    LEFT JOIN {{ ref('th__candidates_status') }} as cs on cs.CandidateId = c.Id
    GROUP BY c.Id, o.Address, cs.ToStatus, cs.TimeDifferenceInHours   
) 

SELECT *
FROM candidates_with_info;
