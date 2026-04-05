from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from datetime import datetime, timedelta

# ==============================
# CONFIG
# ==============================
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "dev"

# ==============================
# SLACK ALERTS
# ==============================
def slack_fail_alert(context):
    return send_slack_notification(
        text=f"""
        ❌ DAG Failed
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution: {context['execution_date']}
        """,
        channel="#data-alerts",
    )(context)

def slack_success_alert(context):
    return send_slack_notification(
        text=f"""
        ✅ DAG Succeeded
        DAG: {context['dag'].dag_id}
        Execution: {context['execution_date']}
        """,
        channel="#data-alerts",
    )(context)

# ==============================
# DEFAULT ARGS
# ==============================
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
    "on_failure_callback": slack_fail_alert,
}

# ==============================
# DAG
# ==============================
with DAG(
    dag_id="healthcare_dbt_snowflake_pipeline_dev",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "healthcare", "dev"],
    on_success_callback=slack_success_alert,
) as dag:

    # ==============================
    # DBT BUILD (STATE-BASED)
    # ==============================
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        mkdir -p {DBT_PROJECT_DIR}/state &&
        cd {DBT_PROJECT_DIR} &&
        dbt build \
            --select state:modified+ \
            --state {DBT_PROJECT_DIR}/state \
            --target {DBT_TARGET} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ==============================
    # SAVE STATE (CRITICAL)
    # ==============================
    save_dbt_state = BashOperator(
        task_id="save_dbt_state",
        bash_command=f"""
        cp {DBT_PROJECT_DIR}/target/manifest.json \
           {DBT_PROJECT_DIR}/state/manifest.json
        """,
    )

    # ==============================
    # SNAPSHOTS
    # ==============================
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt snapshot \
            --target {DBT_TARGET} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ==============================
    # DOCS
    # ==============================
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt docs generate \
            --target {DBT_TARGET} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
        trigger_rule="all_done",
    )

    # ==============================
    # FLOW
    # ==============================
    dbt_build >> save_dbt_state >> dbt_snapshot >> dbt_docs