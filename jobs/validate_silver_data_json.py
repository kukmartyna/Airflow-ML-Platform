import os
import great_expectations as gx
import json
from pyspark.sql import SparkSession


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

data_source_name = "spark"
data_asset_name = "dataframe"

with open("./config/silver_tables_validation_gx.json", "r") as file:
    configs = json.load(file)

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
    context = gx.get_context()

    data_source = context.sources.add_or_update_spark(name=data_source_name)

    try:
        data_asset = data_source.get_asset(data_asset_name)
    except Exception:
        data_asset = data_source.add_dataframe_asset(data_asset_name)

    for config in configs:
        catalog = config['catalog_name']
        schema = config['schema_name']
        table = config['table_name']
        suite_name = config['suite_name']
        checkpoint_name = config['checkpoint_name']
        table_path = f"s3a://{MINIO_BUCKET}/{catalog}/{schema}/{table}"

        df = spark.read.format("delta").load(table_path)

        df.show(truncate=False)

        batch_req = data_asset.build_batch_request(dataframe=df)

        context.add_or_update_checkpoint(
            name=checkpoint_name,
            config_version=1.0,
            run_name_template=table,
            validations=[
                {
                    "batch_request": batch_req,
                }
            ],
            expectation_suite_name=suite_name
        )

        context.run_checkpoint(
            checkpoint_name=checkpoint_name
        )

    index_urls = context.build_data_docs()
    print("📊 Data Docs URLs:", index_urls, flush=True)

