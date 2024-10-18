{% snapshot candidates_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'id',
        strategy = 'timestamp',
        updated_at = 'updatedat'
    )
}}

select * from {{ source('internal_app', 'candidates')}}

{% endsnapshot %}