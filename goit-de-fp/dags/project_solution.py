from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DAGS_DIR = "/opt/airflow/dags"

default_args = {
    "owner": "oleksii_shcherbak",
    "start_date": datetime(2026, 4, 13, 0, 0),
}

with DAG(
    dag_id="oleksii_shcherbak_project_solution",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["oleksii_shcherbak"],
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"python3 {DAGS_DIR}/landing_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"python3 {DAGS_DIR}/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"python3 {DAGS_DIR}/silver_to_gold.py",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
