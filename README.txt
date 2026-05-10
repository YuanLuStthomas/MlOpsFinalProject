MLOps Final Project – Building an Asynchronous AI Inference System
Stack: Airflow + S3 + SQS + Kubernetes (kind) + Docker

0) What this project does (High-level)
This system runs ML inference asynchronously:
1) Train DAG trains a model (breast cancer Logistic Regression) and uploads artifacts to S3
2) Enqueue DAG sends many inference requests to SQS (one message per record)
3) Consumers run in Kubernetes (kind), poll SQS, run inference using model from S3, write prediction JSON to S3, then delete message after success

1) Repo Layout
- dags/train_to_s3_dag.py : DAG train_model_to_s3 (train + upload artifacts to S3)
- dags/enqueue_test_to_sqs_dag.py : DAG enqueue_test_to_sqs (enqueue N test messages to SQS)
- src/training/train.py : training logic + S3 upload
- src/producer/enqueue.py : enqueue logic (SQS send-message)
- src/consumer/worker.py : consumer worker (poll SQS -> infer -> write S3 -> delete message)
- Dockerfile : builds consumer image mlops-final-consumer:latest
- k8s/consumer-deployment.yaml : Kubernetes deployment for consumers
- setup_airflow.sh : project-local Airflow setup
- setup_env.sh : AWS/S3/SQS env vars
- kind-config.yaml : kind cluster config
- writeup.md : short written report

2) Prerequisites (AWS Cloud9)
Quick check:
aws sts get-caller-identity
docker --version
kubectl version --client
kind version

3) Environment setup
3.1 Python venv + deps
cd ~/environment/MlOpsFinalProject
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

3.2 Setup environment variables
setup_env.sh example:
export AWS_REGION="us-east-1"
export PROJECT_BUCKET="mlops-final-yuanlu-20260330"
export QUEUE_NAME="mlops-final-inference-queue"
export QUEUE_URL="https://sqs.us-east-1.amazonaws.com/<ACCOUNT_ID>/mlops-final-inference-queue"
export MAX_ENQUEUE=50

Load:
source ./setup_env.sh

4) Airflow (project-isolated)
4.1 Setup Airflow metadata DB + admin user
cd ~/environment/MlOpsFinalProject
source .venv/bin/activate
source ./setup_env.sh
source ./setup_airflow.sh
airflow db migrate
airflow dags list-import-errors

4.2 Run Airflow (2 terminals)
Terminal A (Scheduler):
cd ~/environment/MlOpsFinalProject
source .venv/bin/activate
source ./setup_env.sh
source ./setup_airflow.sh
airflow scheduler

Terminal B (Webserver):
cd ~/environment/MlOpsFinalProject
source .venv/bin/activate
source ./setup_env.sh
source ./setup_airflow.sh
airflow webserver --host 0.0.0.0 --port 8080

Airflow UI:
http://<CLOUD9_PUBLIC_IP>:8080
Login: admin / admin

5) Step A – Train model and upload to S3
Trigger DAG: train_model_to_s3 (Airflow UI)

Verify S3 artifacts:
source ./setup_env.sh
aws s3 ls "s3://$PROJECT_BUCKET/models/"
aws s3 cp "s3://$PROJECT_BUCKET/models/metrics.json" -

Expected:
- s3://$PROJECT_BUCKET/models/model.pkl
- s3://$PROJECT_BUCKET/models/metrics.json

6) Step B – Enqueue inference requests to SQS
Set batch size (optional):
export MAX_ENQUEUE=50

Trigger DAG: enqueue_test_to_sqs (Airflow UI)

Verify SQS depth:
source ./setup_env.sh
aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed

7) Step C – Kubernetes consumers (kind)
7.1 Create kind cluster
cd ~/environment/MlOpsFinalProject
kind create cluster --name mlops-final --config kind-config.yaml
kubectl get nodes

7.2 Build consumer image and load into kind
cd ~/environment/MlOpsFinalProject
docker build -t mlops-final-consumer:latest .
kind load docker-image mlops-final-consumer:latest --name mlops-final

7.3 Create AWS creds Secret for pods (IMPORTANT)
voclabs uses session tokens; recreate if consumer fails with ExpiredToken.

source ./setup_env.sh
kubectl delete secret aws-creds --ignore-not-found
kubectl create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)" \
  --from-literal=AWS_SESSION_TOKEN="$(aws configure get aws_session_token)" \
  --from-literal=AWS_REGION="$AWS_REGION" \
  --from-literal=AWS_DEFAULT_REGION="$AWS_REGION"

7.4 Deploy consumer
kubectl apply -f k8s/consumer-deployment.yaml
kubectl get pods -l app=mlops-final-consumer

7.5 Scale consumers
kubectl scale deployment mlops-final-consumer --replicas=3
kubectl get deploy mlops-final-consumer
kubectl get pods -l app=mlops-final-consumer

7.6 Logs
kubectl logs -l app=mlops-final-consumer --tail=100

8) Step D – End-to-end verification
8.1 SQS drains to zero
source ./setup_env.sh
aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed

Expected eventually:
ApproximateNumberOfMessages = 0

8.2 Predictions written to S3
source ./setup_env.sh
aws s3 ls "s3://$PROJECT_BUCKET/predictions/" | head
aws s3 ls "s3://$PROJECT_BUCKET/predictions/" | wc -l

Expected:
Many sample_XXXX.json files; count matches MAX_ENQUEUE





