import os
import json
import joblib
import boto3
from datetime import datetime
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score


def train_and_upload(bucket: str, model_key: str, metrics_key: str) -> dict:
    # Load dataset
    data = load_breast_cancer()
    X = data.data
    y = data.target

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Train
    clf = LogisticRegression(max_iter=2000)
    clf.fit(X_train, y_train)

    # Eval (optional but useful)
    preds = clf.predict(X_test)
    acc = float(accuracy_score(y_test, preds))

    # Save artifacts locally
    os.makedirs("models", exist_ok=True)
    local_model_path = "models/model.pkl"
    joblib.dump(clf, local_model_path)

    metrics = {
        "accuracy": acc,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dataset": "breast_cancer",
        "model_type": "logistic_regression",
    }
    local_metrics_path = "models/metrics.json"
    with open(local_metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    # Upload to S3
    s3 = boto3.client("s3")
    s3.upload_file(local_model_path, bucket, model_key)
    s3.upload_file(local_metrics_path, bucket, metrics_key)

    print(f"[train] Uploaded model to s3://{bucket}/{model_key}")
    print(f"[train] Uploaded metrics to s3://{bucket}/{metrics_key}")
    print(f"[train] accuracy={acc:.4f}")

    return metrics
