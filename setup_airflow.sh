#!/usr/bin/env bash
set -e

export PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
export AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"

mkdir -p "$AIRFLOW_HOME"

airflow db init >/dev/null

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin >/dev/null 2>&1 || true

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
echo "LOAD_EXAMPLES=$AIRFLOW__CORE__LOAD_EXAMPLES"
