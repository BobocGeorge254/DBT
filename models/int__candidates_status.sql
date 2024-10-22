{{ config(materialized='table') }}

WITH candidates AS (
    SELECT 
        c.Id AS CandidateId,
        c.CreatedAt AS CandidateCreatedAt
    FROM {{ source('internal_app', 'candidates') }} AS c
),
cards AS (
    SELECT 
        ca.Id AS CardId,
        ca.CandidateId
    FROM {{ source('internal_app', 'cards') }} AS ca
),
card_history AS (
    SELECT 
        ch.CardId,
        ch.ToStatus AS ToStatus,
        ch.DateTime AS StatusChangeTimestamp
    FROM {{ ref('int__card_history') }} AS ch
),
status AS (
    SELECT 
        s.value AS StatusValue,
        s.Status
    FROM {{ ref("status") }} AS s
),
candidate_status AS (
    SELECT
        c.CandidateId,
        s.Status AS ToStatus,
        ch.StatusChangeTimestamp,
        c.CandidateCreatedAt,
        ROW_NUMBER() OVER (PARTITION BY c.CandidateId ORDER BY ch.StatusChangeTimestamp DESC) AS rn
    FROM candidates AS c
    LEFT JOIN cards AS ca ON c.CandidateId = ca.CandidateId
    LEFT JOIN card_history AS ch ON ca.CardId = ch.CardId
    LEFT JOIN status AS s ON s.StatusValue = ch.ToStatus
)

SELECT
    CandidateId,
    ToStatus,
    DATEDIFF(HOUR, CandidateCreatedAt, StatusChangeTimestamp) AS TimeDifferenceInHours
FROM candidate_status
WHERE rn = 1;
