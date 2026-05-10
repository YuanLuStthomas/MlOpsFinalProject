import os
import json
import boto3
from datetime import datetime
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


def enqueue_test_set(queue_url: str, max_messages: int = None) -> dict:
    """
    Recreate the SAME deterministic train/test split as training,
    then enqueue one SQS message per test record.

    Message format:
      {"record_id":"sample_001","features":[...]}
    """
    data = load_breast_cancer()
    X = data.data
    y = data.target

    # MUST match training split settings (random_state + stratify)
    _, X_test, _, _ = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    sqs = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))

    sent = 0
    for i, row in enumerate(X_test):
        if max_messages is not None and sent >= int(max_messages):
            break

        record_id = f"sample_{i:04d}"
        body = {"record_id": record_id, "features": row.tolist()}

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(body),
        )
        sent += 1

    return {
        "sent": sent,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "queue_url": queue_url,
    }
