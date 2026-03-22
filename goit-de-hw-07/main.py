import random
import time
from datetime import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

MYSQL_CONN_ID = "mysql_connection_oleksii"
SCHEMA = "neo_data"
TABLE = f"{SCHEMA}.oleksii_shcherbak_medal_counts"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDAL_TYPES = ["Bronze", "Silver", "Gold"]

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 20, 0, 0),
}


def pick_medal():
    chosen = random.choice(MEDAL_TYPES)
    print(f"Chosen medal type: {chosen}")
    return chosen


def branch_on_medal(ti):
    chosen = ti.xcom_pull(task_ids="pick_medal_task")
    branch_map = {
        "Bronze": "count_bronze",
        "Silver": "count_silver",
        "Gold": "count_gold",
    }
    return branch_map[chosen]


def delay_next_task():
    seconds = 5
    print(f"Sleeping for {seconds} seconds to simulate processing delay...")
    time.sleep(seconds)


with DAG(
    dag_id="oleksii_shcherbak_medal_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["oleksii_shcherbak"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_on_medal,
    )

    count_bronze = SQLExecuteQueryOperator(
        task_id="count_bronze",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TABLE} (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Bronze';
        """,
    )

    count_silver = SQLExecuteQueryOperator(
        task_id="count_silver",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TABLE} (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Silver';
        """,
    )

    count_gold = SQLExecuteQueryOperator(
        task_id="count_gold",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TABLE} (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = 'Gold';
        """,
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_next_task,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            WITH recent AS (
                SELECT COUNT(*) AS nrows
                FROM {TABLE}
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT nrows > 0 FROM recent;
        """,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    create_table >> pick_medal_task >> branch_task
    branch_task >> [count_bronze, count_silver, count_gold]
    count_bronze >> delay_task
    count_silver >> delay_task
    count_gold >> delay_task
    delay_task >> check_recent_record
