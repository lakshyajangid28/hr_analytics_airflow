from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.models import Variable
from datetime import datetime
import requests
import time

# ---------------------------------------------------
# Configuration
# ---------------------------------------------------

ACCOUNT_ID = "70506183150011"
JOB_ID = "70506183135984"

DBT_TOKEN = Variable.get("DBT_TOKEN")

# ---------------------------------------------------
# Snowflake Procedure
# ---------------------------------------------------

def load_employee_data():

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    hook.run(
        "CALL LOAD_EMPLOYEE_DATA();"
    )

    print("Employee data loaded successfully.")


# ---------------------------------------------------
# Trigger dbt Cloud Job
# ---------------------------------------------------

def run_dbt():

    headers = {
        "Authorization": f"Token {DBT_TOKEN}",
        "Content-Type": "application/json"
    }

    run_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/"
        f"{ACCOUNT_ID}/jobs/{JOB_ID}/run/"
    )

    response = requests.post(
        run_url,
        headers=headers,
        json={}
    )

    response.raise_for_status()

    run_id = response.json()["data"]["id"]

    print(f"dbt Run Started: {run_id}")

    while True:

        status_response = requests.get(
            f"https://cloud.getdbt.com/api/v2/accounts/"
            f"{ACCOUNT_ID}/runs/{run_id}/",
            headers=headers
        )

        status_response.raise_for_status()

        status = status_response.json()["data"]["status"]

        if status == 10:
            print("dbt Job Completed Successfully.")
            break

        elif status in [20, 30]:
            raise Exception("dbt Job Failed.")

        else:
            print("dbt Job Running...")
            time.sleep(20)


# ---------------------------------------------------
# DAG
# ---------------------------------------------------

with DAG(

    dag_id="hr_analytics_pipeline",

    start_date=datetime(2026, 7, 1),

    schedule="@daily",

    catchup=False,

    tags=["HR", "Snowflake", "dbt"]

) as dag:

    load_data = PythonOperator(

        task_id="load_employee_data",

        python_callable=load_employee_data

    )

    run_dbt_job = PythonOperator(

        task_id="run_dbt_job",

        python_callable=run_dbt

    )

    load_data >> run_dbt_job