FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir boto3 joblib numpy scikit-learn
COPY src/consumer/worker.py /app/worker.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/worker.py"]
