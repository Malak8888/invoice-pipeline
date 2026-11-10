from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_SUBMIT = "docker exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2"

with DAG(
    dag_id="invoice_late_payment_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["invoices", "mlops"],
) as dag:

    check_data = BashOperator(
        task_id="check_data",
        bash_command=(
            "docker exec namenode hdfs dfs -ls /data/invoices | grep -q parquet "
            "&& echo 'Donnees OK' || (echo 'Aucun Parquet dans HDFS' && exit 1)"
        ),
    )

    train = BashOperator(
        task_id="train",
        bash_command=f"{SPARK_SUBMIT} /opt/ml/train.py",
    )

    train_sklearn = BashOperator(
        task_id="train_sklearn",
        bash_command=f"{SPARK_SUBMIT} /opt/ml/train_sklearn.py",
    )

    promote = BashOperator(
        task_id="promote",
        bash_command=(
            "docker exec spark python3 -u /opt/ml/promote.py invoice_late_payment && "
            "docker exec spark python3 -u /opt/ml/promote.py invoice_late_payment_sk"
        ),
    )

    check_data >> train >> train_sklearn >> promote