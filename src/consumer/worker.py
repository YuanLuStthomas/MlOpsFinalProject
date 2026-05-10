import os, json, time
import boto3
import joblib
import numpy as np
from datetime import datetime
from botocore.exceptions import ClientError

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
PROJECT_BUCKET = os.environ["PROJECT_BUCKET"]
QUEUE_URL = os.environ["QUEUE_URL"]
MODEL_KEY = os.environ.get("MODEL_KEY", "models/model.pkl")
PRED_PREFIX = os.environ.get("PRED_PREFIX", "predictions")

def s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com"
    )

def load_model():
    s3 = s3_client()
    local_path = "/tmp/model.pkl"

    while True:
        try:
            obj = s3.get_object(Bucket=PROJECT_BUCKET, Key=MODEL_KEY)
            body = obj["Body"].read()
            with open(local_path, "wb") as f:
                f.write(body)
            return joblib.load(local_path)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ExpiredToken":
                print("[consumer] WARNING: ExpiredToken while downloading model. Refresh secret & restart deployment. Sleeping 30s...")
                time.sleep(30)
                continue
            print(f"[consumer] ERROR downloading model: {e}")
            time.sleep(5)

def write_result(record_id: str, pred: int):
    s3 = s3_client()
    body = {"record_id": record_id, "prediction": int(pred), "timestamp": datetime.utcnow().isoformat() + "Z"}
    key = f"{PRED_PREFIX}/{record_id}.json"
    s3.put_object(Bucket=PROJECT_BUCKET, Key=key, Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
    return key

def main():
    print("[consumer] starting...")
    model = load_model()
    print(f"[consumer] model loaded from s3://{PROJECT_BUCKET}/{MODEL_KEY} region={AWS_REGION}")

    sqs = boto3.client("sqs", region_name=AWS_REGION)

    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                VisibilityTimeout=30,
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ExpiredToken":
                print("[consumer] WARNING: ExpiredToken receiving from SQS. Refresh secret & restart deployment. Sleeping 30s...")
                time.sleep(30)
                continue
            print(f"[consumer] ERROR receive_message: {e}")
            time.sleep(5)
            continue

        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        msg = msgs[0]
        receipt = msg["ReceiptHandle"]

        try:
            payload = json.loads(msg["Body"])
            record_id = payload["record_id"]
            features = np.array(payload["features"], dtype=float).reshape(1, -1)

            pred = model.predict(features)[0]
            key = write_result(record_id, int(pred))

            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            print(f"[consumer] ok {record_id} -> {int(pred)} wrote s3://{PROJECT_BUCKET}/{key}")

        except Exception as e:
            print(f"[consumer] ERROR processing message: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
