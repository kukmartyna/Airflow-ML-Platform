from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.3.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

with DAG(
    dag_id="clean_titanic_data",
    schedule=None,
    catchup=False,
) as dag:
    read_bronze_data_task = SparkSubmitOperator(
        task_id="clean_titanic_data",
        application="./jobs/clean_titanic_data.py",
        conn_id='spark_default',
        packages=PACKAGES,
        name="my_spark_job",
        verbose=True,  # enable for debugging
    )

