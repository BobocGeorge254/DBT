{{ config(materialized='table') }}

SELECT
    c.Id AS CardId,
    h.fromStatus AS FromStatus,
    h.toStatus AS ToStatus,
    TRY_CAST(h.timestamp AS DATETIME2) AS DateTime,
    h.userId AS CreatedById
FROM {{ source('internal_app', 'cards') }} AS c
CROSS APPLY OPENJSON(c.CardHistoryJson) 
WITH (
    fromStatus INT '$.fromStatus',
    toStatus INT '$.toStatus',
    timestamp NVARCHAR(50) '$.timestamp',
    userId UNIQUEIDENTIFIER '$.userId'
) AS h;
