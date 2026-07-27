from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import time
from airflow.models import Variable

# -----------------------------
# Configuration
# -----------------------------

ACCOUNT_ID = "70506183150011"
JOB_ID = "70506183135984"

DBT_TOKEN = Variable.get("DBT_TOKEN")

SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")

# -----------------------------
# Procedure Execution
# -----------------------------

def load_employee_data():

    import snowflake.connector

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse="COMPUTE_WH",
        database="HR_ANALYTICS_DB",
        schema="RAW"
    )

    cur = conn.cursor()

    cur.execute("CALL LOAD_EMPLOYEE_DATA();")

    print(cur.fetchone())

    cur.close()
    conn.close()


# -----------------------------
# Trigger dbt Job
# -----------------------------

def run_dbt():

    headers = {
        "Authorization": f"Token {DBT_TOKEN}",
        "Content-Type": "application/json"
    }

    url = f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"

    response = requests.post(
        url,
        headers=headers,
        json={}
    )

    response.raise_for_status()

    run_id = response.json()["data"]["id"]

    print(f"dbt Run Started : {run_id}")

    while True:

        status = requests.get(
            f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/runs/{run_id}/",
            headers=headers
        ).json()["data"]["status"]

        if status == 10:
            print("dbt Success")
            break

        elif status in [20, 30]:
            raise Exception("dbt Failed")

        else:
            time.sleep(20)


# -----------------------------
# DAG
# -----------------------------

with DAG(

    dag_id="hr_analytics_pipeline",

    start_date=datetime(2026,7,1),

    schedule="@daily",

    catchup=False,

    tags=["HR","Snowflake","dbt"]

) as dag:

    load = PythonOperator(

        task_id="load_employee_data",

        python_callable=load_employee_data

    )

    dbt = PythonOperator(

        task_id="run_dbt",

        python_callable=run_dbt

    )

    load >> dbt