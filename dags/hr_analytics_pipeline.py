from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime

import requests
import time

# -----------------------------
# Configuration
# -----------------------------

ACCOUNT_ID = "70506183150011"
JOB_ID = "70506183135984"

# -----------------------------
# Snowflake Procedure Execution
# -----------------------------

def load_employee_data():

    import snowflake.connector

    snowflake_user = Variable.get("SNOWFLAKE_USER")
    snowflake_password = Variable.get("SNOWFLAKE_PASSWORD")
    snowflake_account = Variable.get("SNOWFLAKE_ACCOUNT")

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse="COMPUTE_WH",
        database="HR_ANALYTICS_DB",
        schema="RAW"
    )

    try:

        cur = conn.cursor()

        cur.execute("CALL LOAD_EMPLOYEE_DATA();")

        result = cur.fetchone()

        print(f"Procedure Result: {result}")

        cur.close()

    finally:

        conn.close()


# -----------------------------
# Trigger dbt Cloud Job
# -----------------------------

def run_dbt():

    dbt_token = Variable.get("DBT_TOKEN")

    headers = {
        "Authorization": f"Token {dbt_token}",
        "Content-Type": "application/json"
    }

    run_url = (
        f"https://ef420.us1.dbt.com/api/v2/accounts/"
        f"{ACCOUNT_ID}/jobs/{JOB_ID}/run/"
    )

    response = requests.post(
        run_url,
        headers=headers,
        json={
            "cause": "Triggered by Airflow"
        }
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to trigger dbt job. "
            f"Status Code: {response.status_code}. "
            f"Response: {response.text}"
        )

    run_id = response.json()["data"]["id"]

    while True:

        status_response = requests.get(
            f"https://ef420.us1.dbt.com/api/v2/accounts/"
            f"{ACCOUNT_ID}/runs/{run_id}/",
            headers=headers
        )

        if status_response.status_code != 200:
            raise Exception(
                f"Failed to fetch dbt run status. "
                f"Status Code: {status_response.status_code}. "
                f"Response: {status_response.text}"
            )

        status = status_response.json()["data"]["status"]

        # Success
        if status == 10:
            return

        # Failed / Cancelled
        elif status in [20, 30]:
            raise Exception(
                f"dbt run failed. Run ID: {run_id}. "
                f"Response: {status_response.text}"
            )

        time.sleep(20)


# -----------------------------
# DAG Definition
# -----------------------------

with DAG(
    dag_id="hr_analytics_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["HR", "Snowflake", "dbt"]
) as dag:

    load_employee_task = PythonOperator(
        task_id="load_employee_data",
        python_callable=load_employee_data
    )

    dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt
    )

    success_email = EmailOperator(
        task_id="success_email",
        to="YOUR_EMAIL@gmail.com",
        subject="✅ HR Analytics Pipeline Completed Successfully",
        html_content="""
        <h2>HR Analytics Pipeline Success</h2>

        <p>The Airflow pipeline completed successfully.</p>

        <ul>
            <li>Snowflake Procedure Executed ✅</li>
            <li>dbt Run Completed ✅</li>
            <li>dbt Snapshot Completed ✅</li>
        </ul>

        <p><strong>DAG:</strong> hr_analytics_pipeline</p>
        <p><strong>Execution Time:</strong> {{ ts }}</p>

        <p>Regards,<br>
        Airflow / Astronomer</p>
        """
    )

    failure_email = EmailOperator(
        task_id="failure_email",
        to="YOUR_EMAIL@gmail.com",
        subject="❌ HR Analytics Pipeline Failed",
        html_content="""
        <h2>HR Analytics Pipeline Failed</h2>

        <p>One or more tasks in the pipeline failed.</p>

        <ul>
            <li>DAG: hr_analytics_pipeline</li>
            <li>Execution Time: {{ ts }}</li>
        </ul>

        <p>Please review the Airflow/Astronomer logs for details.</p>

        <p>Regards,<br>
        Airflow / Astronomer</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    load_employee_task >> dbt_task

    dbt_task >> success_email

    [load_employee_task, dbt_task] >> failure_email