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
        CATALOG = 'bronze'
        SCHEMA = 'public'

        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/public/klienci"
        silver_path = f"s3a://{MINIO_BUCKET}/silver/public/klienci"

        df = spark.read.format("delta").load(bronze_path)
        df.show()

        df = df.withColumn("payload", F.col("after"))

        df = df.select(
            F.col("payload"),
            F.col("source.lsn"),
            F.col("op"),
        )

        pk = ['payload.id_klienta']
        w = Window.partitionBy(*pk).orderBy(F.col("lsn").desc_nulls_last())

        df = df.withColumn("rn", F.row_number().over(w))
        df.show()
        df = df.filter(
            (F.col("rn") == 1)
            & (F.col("op") != 'd')
        )
        df = df.select(F.col("payload.*"))

        df.show()

        # before = df.select(F.col('before.*'))
        # after = df.select(F.col('after.*'))
        # source = df.select(F.col('source.*'))

        # before.show()
        # after.show()
        # source.show()

        df.write.format("json").mode("overwrite").save(silver_path)


if __name__ == "__main__":
    main()
