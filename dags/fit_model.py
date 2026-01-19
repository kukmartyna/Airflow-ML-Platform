import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv

load_dotenv()

PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.3.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

with DAG(
    dag_id="fit_model",
    schedule=None,
    catchup=False,
) as dag:
    fit_model = SparkSubmitOperator(
        task_id="fit_model",
        application="./jobs/fit_model.py",
        conn_id='spark_default',
        packages=PACKAGES,
        name="my_spark_job",
        env_vars={
            "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", ""),
            "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", ""),
            "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", ""),
            "MINIO_BUCKET": os.getenv("MINIO_BUCKET", "")
        },
        #verbose=True,  # enable for debugging
    )

