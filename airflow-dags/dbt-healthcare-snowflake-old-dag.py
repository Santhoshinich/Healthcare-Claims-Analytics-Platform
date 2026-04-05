from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.notifications.slack import send_slack_notification
from datetime import datetime, timedelta

# ==============================
# CONFIG
# ==============================
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "dev"   # change to dev if needed

# ==============================
# ALERTING
# ==============================
def slack_fail_alert(context):
    return send_slack_notification(
        text=f"""
        ❌ *DAG Failed*
        *DAG*: {context['dag'].dag_id}
        *Task*: {context['task_instance'].task_id}
        *Execution Time*: {context['execution_date']}
        """,
        channel="#data-alerts",
    )(context)


def slack_success_alert(context):
    return send_slack_notification(
        text=f"""
        ✅ *DAG Succeeded*
        *DAG*: {context['dag'].dag_id}
        *Execution Time*: {context['execution_date']}
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
    dag_id="healthcare_dbt_pipeline_prod",
    default_args=default_args,
    description="Production dbt pipeline for healthcare analytics",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "healthcare", "prod"],
    on_success_callback=slack_success_alert,
) as dag:

    # ==============================
    # DBT DEPENDENCIES
    # ==============================
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt deps
        """,
    )

    # ==============================
    # STAGING LAYER
    # ==============================
    with TaskGroup("staging_layer") as staging_layer:

        dbt_run_staging = BashOperator(
            task_id="run_staging_models",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --select staging --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
            """,
        )

        dbt_test_staging = BashOperator(
            task_id="test_staging_models",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt test --select staging --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
            """,
        )

        dbt_run_staging >> dbt_test_staging

    # ==============================
    # MARTS LAYER
    # ==============================
    with TaskGroup("marts_layer") as marts_layer:

        dbt_run_marts = BashOperator(
            task_id="run_marts_models",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --select marts --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
            """,
        )

        dbt_test_marts = BashOperator(
            task_id="test_marts_models",
            bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt test --select marts --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
            """,
        )

        dbt_run_marts >> dbt_test_marts

    # ==============================
    # SNAPSHOTS
    # ==============================
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt snapshot --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ==============================
    # DOCS (OPTIONAL BUT STRONG)
    # ==============================
    dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt docs generate --target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}
        """,
        trigger_rule="all_done",  # still run even if failures
    )

    # ==============================
    # PIPELINE FLOW
    # ==============================
    dbt_deps >> staging_layer >> marts_layer >> dbt_snapshot >> dbt_docs