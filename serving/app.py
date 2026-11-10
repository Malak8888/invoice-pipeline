import os
import mlflow, mlflow.sklearn
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
MODEL_URI = os.getenv("MODEL_URI", "models:/invoice_late_payment_sk@staging")
FEATURES = ["amount", "due_days", "is_short_term", "issue_month",
            "cust_late_rate", "cust_nb_invoices"]

app = FastAPI(title="Invoice late payment API")
_model = None


def get_model():
    global _model
    if _model is None:
        _model = mlflow.sklearn.load_model(MODEL_URI)
    return _model


class Invoice(BaseModel):
    amount: float
    due_days: int
    issue_month: int = 1
    cust_late_rate: float = 0.2
    cust_nb_invoices: int = 0


@app.get("/health")
def health():
    return {"status": "ok", "model_uri": MODEL_URI}


@app.post("/predict")
def predict(inv: Invoice):
    row = {
        "amount": inv.amount,
        "due_days": inv.due_days,
        "is_short_term": int(inv.due_days == 15),
        "issue_month": inv.issue_month,
        "cust_late_rate": inv.cust_late_rate,
        "cust_nb_invoices": inv.cust_nb_invoices,
    }
    proba = float(get_model().predict_proba(pd.DataFrame([row])[FEATURES])[0, 1])
    return {"late_probability": round(proba, 4), "predicted_late": proba >= 0.5}