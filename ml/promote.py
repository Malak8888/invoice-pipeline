import sys
import mlflow
from mlflow import MlflowClient

mlflow.set_tracking_uri("http://mlflow:5000")
client = MlflowClient()

name = sys.argv[1] if len(sys.argv) > 1 else "invoice_late_payment"
latest = max(client.search_model_versions(f"name='{name}'"),
             key=lambda v: int(v.version))

client.set_registered_model_alias(name, "staging", latest.version)
print(f"{name} v{latest.version} -> alias 'staging'")