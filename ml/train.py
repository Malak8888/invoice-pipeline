from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import mlflow, mlflow.spark

spark = SparkSession.builder.appName("train_invoices").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Lecture depuis HDFS
df = spark.read.parquet("hdfs://namenode:9000/data/invoices")

# 2. Dédoublonnage et features
df = df.dropDuplicates(["invoice_id"]).dropna()
df = df.withColumn("label", F.col("paid_late").cast("int"))
df = df.withColumn("is_short_term", (F.col("due_days") == 15).cast("int"))
df = df.withColumn("issue_month", F.month("issued_at"))

# Séparation AVANT le calcul de l'historique client (évite la fuite de données)
train, test = df.randomSplit([0.8, 0.2], seed=42)

cust_stats = (train.groupBy("customer_id")
              .agg(F.avg("label").alias("cust_late_rate"),
                   F.count("*").alias("cust_nb_invoices")))
train = train.join(cust_stats, "customer_id", "left")
test = test.join(cust_stats, "customer_id", "left").fillna(
    {"cust_late_rate": 0.2, "cust_nb_invoices": 0})

features = ["amount", "due_days", "is_short_term", "issue_month",
            "cust_late_rate", "cust_nb_invoices"]

# Pas de assembler.transform ici : l'assembler fait partie du Pipeline
train, test = train.cache(), test.cache()
n_train, n_test = train.count(), test.count()

# 3. Entraînement + suivi MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("invoice_late_payment")

params = {"numTrees": 100, "maxDepth": 6, "seed": 42}

with mlflow.start_run(run_name="random_forest"):
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", **params)
    model = Pipeline(stages=[assembler, rf]).fit(train)
    rf_model = model.stages[-1]

    # 4. Évaluation
    pred = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
    f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(pred)

    mlflow.log_params(params)
    mlflow.log_param("features", ",".join(features))
    mlflow.log_param("nb_rows", n_train + n_test)
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("f1", f1)
    importances = rf_model.featureImportances.toArray()
    for name, imp in zip(features, importances):
        mlflow.log_metric(f"importance_{name}", float(imp))

    mlflow.spark.log_model(model, artifact_path="model",
                           registered_model_name="invoice_late_payment")

    print(f"Lignes train: {n_train}, test: {n_test}")
    print(f"AUC = {auc:.4f}")
    print(f"F1  = {f1:.4f}")
    for name, imp in sorted(zip(features, importances), key=lambda x: -x[1]):
        print(f"{name:20s} {imp:.3f}")