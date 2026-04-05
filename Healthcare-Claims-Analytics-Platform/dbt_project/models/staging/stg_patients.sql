SELECT
    patient_id,
    name,
    dob,
    gender,
    city
FROM {{ source('raw', 'patients') }}