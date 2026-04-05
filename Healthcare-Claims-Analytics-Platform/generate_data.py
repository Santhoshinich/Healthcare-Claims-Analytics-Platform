import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()

NUM_PATIENTS = 1000
NUM_PROVIDERS = 100
NUM_CLAIMS = 10000

# Patients
patients = []
for i in range(NUM_PATIENTS):
    patients.append({
        "patient_id": i,
        "name": fake.name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "gender": random.choice(["M", "F"]),
        "city": fake.city()
    })

df_patients = pd.DataFrame(patients)

# Providers
providers = []
for i in range(NUM_PROVIDERS):
    providers.append({
        "provider_id": i,
        "name": fake.company(),
        "specialty": random.choice(["Cardiology", "Dermatology", "Orthopedics"]),
        "location": fake.city()
    })

df_providers = pd.DataFrame(providers)

# Claims
claims = []
for i in range(NUM_CLAIMS):
    amount = round(np.random.exponential(scale=5000), 2)

    claims.append({
        "claim_id": i,
        "patient_id": random.randint(0, NUM_PATIENTS - 1),
        "provider_id": random.randint(0, NUM_PROVIDERS - 1),
        "claim_amount": amount,
        "diagnosis_code": f"D{random.randint(100,999)}",
        "procedure_code": f"P{random.randint(100,999)}",
        "claim_date": fake.date_this_year(),
        "status": random.choice(["Approved", "Denied", "Pending"])
    })

df_claims = pd.DataFrame(claims)

# Save
df_patients.to_csv("patients.csv", index=False)
df_providers.to_csv("providers.csv", index=False)
df_claims.to_csv("claims.csv", index=False)

print("Data generated")