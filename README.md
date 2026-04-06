# 🏥 Healthcare Claims Analytics Platform

## 🚀 Overview

A production-grade data platform that ingests, transforms, and analyzes healthcare claims data using modern data engineering tools.

This project demonstrates end-to-end pipeline design, orchestration, transformation, and deployment best practices.

---

## 🧱 Architecture

Local Machine
→ Docker (Airflow + dbt)
→ Snowflake Data Warehouse

---

## ⚙️ Tech Stack

* **Orchestration:** Apache Airflow
* **Transformation:** dbt (data build tool)
* **Warehouse:** Snowflake
* **Containerization:** Docker
* **CI/CD:** GitHub Actions

---

## 🔄 Pipeline Flow

1. **Ingestion Layer**

   * Raw data stored in Snowflake (`RAW` schema)

2. **Staging Layer (dbt)**

   * Data cleaning and normalization
   * Models: `stg_claims`, `stg_patients`, `stg_providers`

3. **Analytics Layer**

   * Fact and derived models
   * Models: `fact_claims`, `fraud_detection`

4. **Snapshots**

   * Tracks slowly changing dimensions

5. **Data Quality**

   * dbt tests (not null, uniqueness, relationships)

6. **Orchestration**

   * Airflow DAG with:

     * retries
     * Slack alerts
     * state-based dbt runs

---

## ⚡ Key Features

* ✅ Incremental models for performance optimization
* ✅ State-based dbt execution (`state:modified+`)
* ✅ Automated data testing
* ✅ Snapshotting for historical tracking
* ✅ Dockerized environment
* ✅ CI/CD pipeline with GitHub Actions

---

## 🔐 Security

* Secrets managed via `.env` and GitHub Secrets
* No credentials stored in code

---

## 🧪 Running Locally

```bash
docker-compose up -d
```

Access Airflow:

```
http://localhost:8081
```

---

## 🔄 CI/CD

* Triggered on Pull Requests
* Runs:

  * `dbt debug`
  * `dbt build`
* Validates pipeline before merge


---

### 🔁 How it works (TL;DR)
- Airflow triggers dbt `build` (state-based)
- dbt runs staging → marts → tests → snapshots
- Results land in Snowflake (DBT_STAGING / DBT_ANALYTICS)
- CI validates every PR with `dbt build`


---


## 📊 Future Improvements

* Data observability dashboards
* Advanced dimensional modeling
* Cost optimization strategies
* Multi-tenant SaaS architecture

---

## 👨‍💻 Author

Santhoshini Ch

Built as a production-grade data engineering project to demonstrate:

* Pipeline design
* Data modeling
* Performance optimization
* Deployment readiness
