{{ config(materialized='table') }}

WITH cards AS (
    SELECT 
        c.Id AS CardId,
        c.CardHistoryJson
    FROM {{ source('internal_app', 'cards') }} AS c
),
card_history AS (
    SELECT
        CardId,
        h.fromStatus AS FromStatus,
        h.toStatus AS ToStatus,
        TRY_CAST(h.timestamp AS DATETIME2) AS DateTime,
        h.userId AS CreatedById
    FROM cards AS c
    CROSS APPLY OPENJSON(c.CardHistoryJson) 
    WITH (
        fromStatus INT '$.fromStatus',
        toStatus INT '$.toStatus',
        timestamp NVARCHAR(50) '$.timestamp',
        userId UNIQUEIDENTIFIER '$.userId'
    ) AS h
)

SELECT 
    CardId,
    FromStatus,
    ToStatus,
    DateTime,
    CreatedById
FROM card_history;
