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


def v():
    MINIO_ENDPOINT = 'http://host.docker.internal:9000'
    MINIO_ACCESS_KEY = 'admin'
    MINIO_SECRET_KEY = 'password'
    MINIO_BUCKET = 'default'

    # GX configuration
    context = gx.get_context()

    data_source = context.sources.add_spark(
        name='spark'
    )

    data_asset = data_source.add_dataframe_asset('dataframe')

    with (SparkSession.builder
            .appName('PySpark example')
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()) as spark:
        schema = 'public'
        table = 'klienci'

        silver_path = f"s3a://{MINIO_BUCKET}/silver/{schema}/{table}"

        df = spark.read.format("delta").load(silver_path)

        df.show()

        batch_req = data_asset.build_batch_request(dataframe=df)

        validator = context.get_validator(
            batch_request=batch_req,
            expectation_suite_name="expect_suite",
        )

        validator.expect_column_values_to_not_be_null(column='id_klienta')

        validator.save_expectation_suite(discard_failed_expectations=False)

        checkpoint_name = "checkpoint"
        yaml_config = f"""
            name : {checkpoint_name}
            config_version: 1.0
            class_name: SimpleCheckpoint
            run_name_template: "abcd"
            validations:
                - batch_request:
                    datasource_name: spark
                    data_asset_name: dataframe
            expectation_suite_name: expect_suite
            """

        check = context.test_yaml_config(yaml_config=yaml_config)

        context.add_checkpoint(checkpoint=check)

        checkpoint_run_result = context.run_checkpoint(
            checkpoint_name="checkpoint"
        )

        print(checkpoint_run_result)


with DAG(
        dag_id="validation_job",
        schedule=None,
        catchup=False,
) as dag:
    read_bronze_data_task = SparkSubmitOperator(
        task_id="validate",
        application="./jobs/validate_data.py",
        conn_id='spark_default',
        packages=PACKAGES,
        name="my_spark_job_test",
        env_vars={
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_ENDPOINT_URL": "http://host.docker.internal:9000",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
        },
        verbose=True,  # enable for debugging
    )
    # validate = PythonOperator(
    #     task_id='test',
    #     python_callable=v
    # )
