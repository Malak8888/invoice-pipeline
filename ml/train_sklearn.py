import mlflow, mlflow.sklearn
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, f1_score
from sklearn.model_selection import train_test_split

# 1. Lecture HDFS avec Spark, puis conversion en pandas
spark = SparkSession.builder.appName("train_sklearn").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sdf = (spark.read.parquet("hdfs://namenode:9000/data/invoices")
       .dropDuplicates(["invoice_id"]).dropna()
       .select("customer_id", "amount", "due_days", "issued_at", "paid_late"))
df = sdf.toPandas()
spark.stop()

# 2. Features
df["label"] = df["paid_late"].astype(int)
df["is_short_term"] = (df["due_days"] == 15).astype(int)
df["issue_month"] = pd.to_datetime(df["issued_at"]).dt.month

train, test = train_test_split(df, test_size=0.2, random_state=42, stratify=df["label"])

# Historique client calculé sur le train uniquement
stats = (train.groupby("customer_id")["label"]
         .agg(cust_late_rate="mean", cust_nb_invoices="count").reset_index())
train = train.merge(stats, on="customer_id", how="left")
test = test.merge(stats, on="customer_id", how="left").fillna(
    {"cust_late_rate": 0.2, "cust_nb_invoices": 0})

features = ["amount", "due_days", "is_short_term", "issue_month",
            "cust_late_rate", "cust_nb_invoices"]

# 3. Entraînement + MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("invoice_late_payment")
params = {"n_estimators": 100, "max_depth": 6, "random_state": 42, "n_jobs": -1}

with mlflow.start_run(run_name="random_forest_sklearn"):
    model = RandomForestClassifier(**params).fit(train[features], train["label"])
    proba = model.predict_proba(test[features])[:, 1]
    auc = roc_auc_score(test["label"], proba)
    f1 = f1_score(test["label"], proba >= 0.5)

    mlflow.log_params(params)
    mlflow.log_param("features", ",".join(features))
    mlflow.log_param("nb_rows", len(df))
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("f1", f1)
    mlflow.sklearn.log_model(model, artifact_path="model",
                             registered_model_name="invoice_late_payment_sk")
    print(f"Lignes train: {len(train)}, test: {len(test)}")
    print(f"AUC = {auc:.4f}  F1 = {f1:.4f}")