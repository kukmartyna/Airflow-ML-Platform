from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

import great_expectations as gx
from pyspark.sql import SparkSession

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.3.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])


with (DAG(
        dag_id="validation_klienci",
        # default_args=DEFAULT_ARGS,
        schedule=None,
        catchup=False)
as dag):
    validate = SparkSubmitOperator(
        task_id="validate_klienci_json",
        application="./jobs/validate_data_json.py",
        conn_id="spark_default",
        packages=PACKAGES,
        name="validation_klienci",
        env_vars={
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_ENDPOINT_URL": "http://host.docker.internal:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
            "GX_SUITE_NAME": "dimklienci_suite",
            "GX_SUITE_PATH": "/opt/airflow/gx/expectations/dimklienci_suite.json",
        },
        verbose=True,
    )

