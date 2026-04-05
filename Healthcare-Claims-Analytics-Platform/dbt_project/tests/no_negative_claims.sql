SELECT *
FROM {{ ref('fact_claims') }}
WHERE claim_amount < 0