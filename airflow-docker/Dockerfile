FROM apache/airflow:2.8.1-python3.10

USER root

# Install system deps if needed
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean

USER airflow

# Install dbt + adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-snowflake==1.7.2

# Optional: install extras
RUN pip install \
    apache-airflow-providers-slack \
    apache-airflow-providers-snowflake