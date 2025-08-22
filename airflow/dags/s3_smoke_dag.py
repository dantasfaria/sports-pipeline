# airflow/dags/s3_smoke_dag.py
from datetime import datetime, timedelta
import os, json
import boto3
from botocore.config import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

def write_test_to_s3():
    # Defaults if .env isn't mounted
    bucket = os.getenv("S3_BUCKET", "sports")
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
    region = os.getenv("AWS_REGION", "us-east-1")
    key = f"bronze/airflow_smoke/dt={datetime.utcnow().date()}/hello_from_airflow.json"

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=region,
        endpoint_url=endpoint,
        config=Config(s3={"addressing_style": "path"})
    )

    # ensure bucket
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    payload = {"msg": "hello from airflow", "ts_utc": datetime.utcnow().isoformat()}
    s3.Object(bucket, key).put(Body=json.dumps(payload).encode("utf-8"))
    print("Wrote", f"s3://{bucket}/{key}")

default_args = {"owner": "you", "retries": 0, "retry_delay": timedelta(seconds=5)}

with DAG(
    dag_id="s3_smoke_dag",
    start_date=datetime(2025, 8, 19),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["smoke"],
) as dag:
    write = PythonOperator(task_id="write_test_to_s3", python_callable=write_test_to_s3)
