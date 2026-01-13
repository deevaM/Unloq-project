#!/bin/bash
set -e

SPARK_SUBMIT=/opt/spark/bin/spark-submit

echo "===== Running read_users.py ====="
$SPARK_SUBMIT /app/spark/read_users.py

echo "===== Running read_events.py ====="
$SPARK_SUBMIT /app/spark/read_events.py

echo "===== Running daily_aggregation.py ====="
$SPARK_SUBMIT /app/spark/daily_aggregation.py

echo "===== Pipeline finished successfully ====="
