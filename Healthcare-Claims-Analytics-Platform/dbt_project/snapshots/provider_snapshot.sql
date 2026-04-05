{% snapshot provider_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='provider_id',
        strategy='check',
        check_cols=['name', 'specialty', 'location']
    )
}}

SELECT *
FROM {{ ref('stg_providers') }}

{% endsnapshot %}