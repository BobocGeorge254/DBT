{{ config(materialized='table') }}

WITH card_history AS (
    SELECT
        ch.CardId,
        ch.DateTime,
        ch.ToStatus AS status
    FROM {{ ref('int__card_history') }} ch
),
cards AS (
    SELECT 
        c.Id AS CardId
    FROM {{ source('internal_app', 'cards') }} c
),
assigneecards AS (
    SELECT 
        ac.CardId,
        ac.AssigneeId AS interviewer_id
    FROM {{ source('internal_app', 'assigneecards') }} ac
),
card_status_durations AS (
    SELECT
        ch.CardId,
        ac.interviewer_id,
        DATEADD(MONTH, DATEDIFF(MONTH, 0, ch.DateTime), 0) AS month,  
        ch.status,
        LEAD(ch.DateTime) OVER (PARTITION BY ch.CardId ORDER BY ch.DateTime) AS next_status_change,
        DATEDIFF(MINUTE, ch.DateTime, LEAD(ch.DateTime) OVER (PARTITION BY ch.CardId ORDER BY ch.DateTime)) AS time_on_status
    FROM card_history ch
    JOIN cards c ON ch.CardId = c.CardId
    JOIN assigneecards ac ON ac.CardId = c.CardId
)

SELECT
    interviewer_id,
    month,
    status,
    COUNT(DISTINCT CardId) AS distinct_cards,
    AVG(time_on_status) AS average_time_on_status
FROM card_status_durations
GROUP BY interviewer_id, month, status;

