from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Import from repo root
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.producer.enqueue import enqueue_test_set

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="enqueue_test_to_sqs",
    default_args=default_args,
    description="Read breast cancer test split and enqueue one message per record to SQS",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def _enqueue():
        queue_url = os.environ["QUEUE_URL"]
        # optional: limit to first N messages for quick test
        max_messages = os.environ.get("MAX_ENQUEUE", None)
        return enqueue_test_set(queue_url=queue_url, max_messages=max_messages)

    enqueue_task = PythonOperator(
        task_id="enqueue_test_records",
        python_callable=_enqueue,
    )

    enqueue_task
