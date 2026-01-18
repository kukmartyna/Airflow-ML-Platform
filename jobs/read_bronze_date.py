import json

from pyspark.sql import SparkSession, Window, functions as F


def main():
    MINIO_ENDPOINT = 'http://host.docker.internal:9000'
    MINIO_ACCESS_KEY = 'admin'
    MINIO_SECRET_KEY = 'password'
    MINIO_BUCKET = 'default'

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

        with open("./config/bronzetosilver.json", "r") as file:
            configs = json.loads(file.read())

        for config in configs:
            catalog = config['catalog_name']
            schema = config['schema_name']
            table = config['table_name']
            pks = config['pks']
            columns = config['columns']

            bronze_path = f"s3a://{MINIO_BUCKET}/{catalog}/{schema}/{table}"
            silver_path = f"s3a://{MINIO_BUCKET}/silver/{schema}/{table}"
            pks = [f'payload.{pk}' for pk in pks]

            df = spark.read.format("delta").load(bronze_path)
            df.show()

            df = df.withColumn("payload", F.col("after"))

            df = df.select(
                F.col("payload"),
                F.col("source.lsn"),
                F.col("op"),
            )

            w = Window.partitionBy(*pks).orderBy(F.col("lsn").desc_nulls_last())

            df = df.withColumn("rn", F.row_number().over(w))
            df.show()
            df = df.filter(
                (F.col("rn") == 1) # biezemy tylko najswiezsza operacje, jesli nie ma delete
                & (F.col("op") != 'd') # jesli ma delete to nic nie robimy
            )
            df = df.select(F.col("payload.*"))

            df.show()

            # before = df.select(F.col('before.*'))
            # after = df.select(F.col('after.*'))
            # source = df.select(F.col('source.*'))

            # before.show()
            # after.show()
            # source.show()

            # Castowanie typów danych
            df.select(
                *[
                    F.col(column["name"]).cast(column["type"]) for column in columns
                ]
            )

            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)


if __name__ == "__main__":
    main()
