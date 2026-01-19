import os
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import NumericType

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

    # ---- MLflow ----
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("gold-train")

    # ---- Load gold features (example expects: label + numeric feature columns) ----
    table_path = f"s3a://{MINIO_BUCKET}/gold/public/titanic"

    df = spark.read.format("delta").load(table_path)

    # Example: all columns except 'label' are numeric features
    label_col = "Survived"
    df = df.withColumn(label_col, df[label_col].cast("double"))

    feature_cols = [
        f.name for f in df.schema.fields
        if f.name != label_col and isinstance(f.dataType, NumericType)
    ]

    for c in ["features", "features_scaled", "label"]:
        if c in df.columns:
            df = df.drop(c)

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vec")
    lr = LogisticRegression(featuresCol="features_vec", labelCol=label_col, maxIter=1)
    pipeline = Pipeline(stages=[assembler, lr])

    with mlflow.start_run(run_name="lr-baseline"):
        mlflow.log_param("table_path", table_path)
        mlflow.log_param("model", "sklearn.LogisticRegression")
        mlflow.log_param("maxIter", 1)
        mlflow.log_param("n_features", len(feature_cols))

        model = pipeline.fit(train_df)

        preds = model.transform(test_df)

        evaluator = BinaryClassificationEvaluator(
            labelCol=label_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        auc = evaluator.evaluate(preds)

        mlflow.log_metric("test_auc", float(auc))
        mlflow.log_metric("train_rows", float(train_df.count()))
        mlflow.log_metric("test_rows", float(test_df.count()))

        # log Spark model to MinIO-backed MLflow artifacts
        mlflow.spark.log_model(
            model,
            artifact_path="model",
            registered_model_name="titanic-logreg"
        )

print("Done. Check MLflow UI for run + model.")
