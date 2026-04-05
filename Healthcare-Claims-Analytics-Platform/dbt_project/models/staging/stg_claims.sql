SELECT
    claim_id,
    patient_id,
    provider_id,
    claim_amount,
    diagnosis_code,
    procedure_code,
    claim_date,
    LOWER(status) AS status
FROM {{ source('raw', 'claims') }}