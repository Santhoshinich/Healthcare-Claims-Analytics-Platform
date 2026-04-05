{{ config(
    materialized='incremental',
    unique_key='claim_id',
    incremental_strategy='merge'
) }}

SELECT
    claim_id,
    patient_id,
    provider_id,
    claim_amount,
    claim_date
FROM {{ ref('stg_claims') }}

{% if is_incremental() %}
WHERE claim_date > COALESCE(
    (SELECT MAX(claim_date) FROM {{ this }}),
    '1900-01-01'
)
{% endif %}