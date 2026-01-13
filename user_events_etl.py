from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SUBMIT = "/opt/bitnami/spark/bin/spark-submit"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="spark_end_to_end_pipeline",
    default_args=default_args,
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["spark", "batch"],
) as dag:

    read_users = BashOperator(
        task_id="read_users",
        bash_command=f"""
        {SPARK_SUBMIT} \
        src/processing/read_users.py
        """
    )

    read_events = BashOperator(
        task_id="read_events",
        bash_command=f"""
        {SPARK_SUBMIT} \
        src/processing/read_event.py
        """
    )

    daily_aggregation = BashOperator(
        task_id="daily_aggregation",
        bash_command=f"""
        {SPARK_SUBMIT} \
        src/aggregation/daily_aggregation.py
        """
    )

    read_users >> read_events >> daily_aggregation
