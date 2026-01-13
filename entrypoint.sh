#!/bin/bash

echo "Initializing Airflow DB..."
airflow db init

echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Running Spark DAG..."
airflow dags trigger spark_end_to_end_pipeline

sleep infinity
