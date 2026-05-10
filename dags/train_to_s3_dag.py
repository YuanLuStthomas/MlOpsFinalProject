from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Make src importable
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.training.train import train_and_upload

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="train_model_to_s3",
    default_args=default_args,
    description="Train breast cancer LR model and upload model.pkl to S3",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def _run_train():
        bucket = os.environ["PROJECT_BUCKET"]
        # fixed location required by project: model.pkl stored in S3
        model_key = "models/model.pkl"
        metrics_key = "models/metrics.json"
        return train_and_upload(bucket=bucket, model_key=model_key, metrics_key=metrics_key)

    train_task = PythonOperator(
        task_id="train_and_upload",
        python_callable=_run_train,
    )

    train_task
