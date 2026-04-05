SELECT
    provider_id,
    name,
    specialty,
    location
FROM {{ source('raw', 'providers') }}