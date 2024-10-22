{{ config(materialized='table') }}

WITH candidates AS (
    SELECT 
        c.Id AS CandidateId,
        c.OfficeId
    FROM {{ source('internal_app', 'candidates') }} AS c
),
offices AS (
    SELECT 
        o.Id AS OfficeId,
        o.Address AS OfficeAddress
    FROM {{ source('internal_app', 'offices') }} AS o
),
cards AS (
    SELECT 
        ca.Id AS CardId,
        ca.CandidateId
    FROM {{ source('internal_app', 'cards') }} AS ca
),
assigneecards AS (
    SELECT 
        ac.Id AS AssigneeCardId,
        ac.CardId
    FROM {{ source('internal_app', 'assigneecards') }} AS ac
),
candidate_status AS (
    SELECT 
        cs.CandidateId,
        cs.ToStatus AS CandidateStatus,
        cs.TimeDifferenceInHours
    FROM {{ ref('int__candidates_status') }} AS cs
),
candidates_with_info AS (
    SELECT
        c.CandidateId,
        COALESCE(o.OfficeAddress, 'UNK') as OfficeAddress,
        COUNT(ac.AssigneeCardId) AS NumberOfAssignees,
        cs.CandidateStatus,
        cs.TimeDifferenceInHours
    FROM candidates AS c
    LEFT JOIN offices AS o ON c.OfficeId = o.OfficeId
    LEFT JOIN cards AS ca ON ca.CandidateId = c.CandidateId
    LEFT JOIN assigneecards AS ac ON ac.CardId = ca.CardId
    LEFT JOIN candidate_status AS cs ON cs.CandidateId = c.CandidateId
    GROUP BY c.CandidateId, o.OfficeAddress, cs.CandidateStatus, cs.TimeDifferenceInHours
)

SELECT
    CandidateId,
    OfficeAddress,
    NumberOfAssignees,
    CandidateStatus,
    TimeDifferenceInHours
FROM candidates_with_info
WHERE CandidateStatus IS NOT NULL

UNION ALL

SELECT
    CandidateId,
    OfficeAddress,
    NumberOfAssignees,
    COALESCE(CandidateStatus, 'Applied') AS CandidateStatus, 
    COALESCE(TimeDifferenceInHours, 0) AS TimeDifferenceInHours 
FROM candidates_with_info
WHERE CandidateStatus IS NULL;
