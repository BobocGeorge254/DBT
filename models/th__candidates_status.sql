WITH candidate_status AS (
    SELECT
        c.Id AS CandidateId,
        s.Status as ToStatus,
        ch.DateTime AS StatusChangeTimestamp,
        c.CreatedAt AS CandidateCreatedAt,
        ROW_NUMBER() OVER (PARTITION BY c.Id ORDER BY ch.DateTime DESC) AS rn  
    FROM {{ source('internal_app', 'candidates') }} AS c  
    LEFT JOIN {{ source('internal_app', 'cards') }} AS ca ON c.Id = ca.CandidateId 
    LEFT JOIN {{ ref('th__card_history') }} AS ch ON ch.CardId = ca.Id
    LEFT JOIN {{ ref("status") }} as s
        on s.value = ch.ToStatus
)

SELECT
    CandidateId,
    ToStatus,
    DATEDIFF(HOUR, CandidateCreatedAt, StatusChangeTimestamp) AS TimeDifferenceInHours
FROM candidate_status c
WHERE rn = 1;  