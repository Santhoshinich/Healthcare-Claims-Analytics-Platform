WITH patient_claims AS (

    SELECT
        patient_id,
        provider_id,
        claim_amount,
        claim_date
    FROM {{ ref('stg_claims') }}

),

aggregated AS (

    SELECT
        patient_id,

        COUNT(*) AS total_claims,

        SUM(claim_amount) AS total_amount,

        AVG(claim_amount) AS avg_claim,

        COUNT(DISTINCT provider_id) AS unique_providers,

        MAX(claim_date) AS last_claim_date,
        MIN(claim_date) AS first_claim_date

    FROM patient_claims
    GROUP BY patient_id
),

final AS (

    SELECT
        *,

        DATEDIFF(day, first_claim_date, last_claim_date) AS active_days,

        CASE
            WHEN avg_claim > 8000 THEN 'HIGH_RISK'
            WHEN total_claims > 50 AND unique_providers < 3 THEN 'SUSPICIOUS_PROVIDER_PATTERN'
            WHEN total_amount > 200000 THEN 'HIGH_SPEND'
            ELSE 'NORMAL'
        END AS risk_flag

    FROM aggregated
)

SELECT * FROM final