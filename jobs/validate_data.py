import great_expectations as gx
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.connect.functions import column

import json


def main():
    MINIO_ENDPOINT = 'http://host.docker.internal:9000'
    MINIO_ACCESS_KEY = 'admin'
    MINIO_SECRET_KEY = 'password'
    MINIO_BUCKET = 'default'

    # os.environ['AWS_ACCESS_KEY'] = MINIO_ACCESS_KEY
    # os.environ['AWS_SECRET_KEY'] = MINIO_SECRET_KEY



    with (SparkSession.builder
            .appName('PySpark exampleeee')
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()) as spark:

        # GX configuration
        context = gx.get_context()

        data_source = context.sources.add_spark(
            name='spark'
        )

        data_asset = data_source.add_dataframe_asset('dataframe')

        schema = 'public'
        table = 'klienci'

        silver_path = f"s3a://{MINIO_BUCKET}/silver/{schema}/{table}"

        df = spark.read.format("delta").load(silver_path)

        df.show()

        batch_req = data_asset.build_batch_request(dataframe=df)

        suite_name = "expect_suite"

        context.add_expectation_suite(expectation_suite_name=suite_name)


        validator = context.get_validator(
            batch_request=batch_req,
            expectation_suite_name=suite_name,
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
        expectation_suite_name: {suite_name}
        """

        check = context.test_yaml_config(yaml_config=yaml_config)

        context.add_checkpoint(checkpoint=check)

        checkpoint_run_result = context.run_checkpoint(
            checkpoint_name="checkpoint"
        )

        print(json.dumps(checkpoint_run_result.to_json_dict(), indent=2))

    # context.add_store(store_name='expectations_S3_store',
    #                   store_config={
    #                       "class_name": "ExpectationsStore",
    #                       "module_name": "great_expectations.data_context.store",
    #                       "store_backend": {
    #                           "class_name": "TupleS3StoreBackend",
    #                           "bucket": "dev-use1-bi-data-warehouse-customer-service-repository",
    #                           "prefix": "data/great_expectations/expectations/"
    #                       }})



if __name__ == '__main__':
    main()
