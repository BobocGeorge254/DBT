{{ config(materialized='table') }}

with card_status_durations as (
    SELECT
        ch.CardId,
        ac.AssigneeId AS interviewer_id,
        DATEADD(MONTH, DATEDIFF(MONTH, 0, ch.DateTime), 0) AS month,  
        ch.ToStatus AS status,
        LEAD(ch.DateTime) OVER (PARTITION BY ch.CardId ORDER BY ch.DateTime) AS next_status_change,
        DATEDIFF(MINUTE, ch.DateTime, LEAD(ch.DateTime) OVER (PARTITION BY ch.CardId ORDER BY ch.DateTime)) AS time_on_status
    from {{ ref('th__card_history') }} ch
    join {{ source('internal_app', 'cards') }} c on ch.CardId = c.Id
    join {{ source('internal_app', 'assigneecards') }} ac on ac.CardId = c.Id
)

select
    interviewer_id,
    month,
    status,
    count(distinct CardId) as distinct_cards,
    avg(time_on_status) as average_time_on_status
from card_status_durations
group by interviewer_id, month, status
