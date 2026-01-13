FROM bitnami/spark:3.5

USER root

# Install Airflow
RUN pip install apache-airflow==2.8.1 pyspark

WORKDIR /app

COPY src/ ./src
COPY data/ ./data
COPY docker/entrypoint.sh .

RUN chmod +x entrypoint.sh

ENV AIRFLOW_HOME=/app/airflow

ENTRYPOINT ["./entrypoint.sh"]
