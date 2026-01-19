import os
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import StringIndexer


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

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
    catalog_silver = "silver"
    catalog_gold = "gold"
    schema = "public"
    table = "titanic"

    silver_path = f"s3a://{MINIO_BUCKET}/{catalog_silver}/{schema}/{table}"
    gold_path = f"s3a://{MINIO_BUCKET}/{catalog_gold}/{schema}/{table}"

    df = spark.read.format("delta").load(silver_path)

    df = df.drop("PassengerId")
    df = df.drop("Cabin")
    df = df.drop("Name")
    df = df.drop("Ticket")

    df = df.filter(F.col("Embarked").isNotNull())

    age_median = df.approxQuantile("Age", [0.5], 0.01)[0]

    df = df.fillna({"Age": age_median})

    sex_indexer = StringIndexer(
        inputCol="Sex", outputCol="Sex_idx", handleInvalid="skip"
    )

    df = (df
          .withColumn("male",
                      F.when(F.col("Sex") == "male", 1).otherwise(0))
          .withColumn("female",
                      F.when(F.col("Sex") == "female", 1).otherwise(0))
          )

    df = (df
          .withColumn("Embarked_Q",
                      F.when(F.col("Embarked") == "Q", 1).otherwise(0))
          .withColumn("Embarked_C",
                      F.when(F.col("Embarked") == "C", 1).otherwise(0))
          .withColumn("Embarked_S",
                      F.when(F.col("Embarked") == "S", 1).otherwise(0))
          )

    df = df.select(
        "Survived",
        "Pclass",
        "Age",
        "SibSp",
        "Parch",
        "Fare",
        "male",
        "female",
        "Embarked_Q",
        "Embarked_C",
        "Embarked_S"
    )
    
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)


