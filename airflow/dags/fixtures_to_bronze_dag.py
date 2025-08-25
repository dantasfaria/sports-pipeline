from datetime import datetime, timedelta, timezone
import os, json
import boto3
from botocore.config import Config
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
if "/opt/airflow/plugins" not in sys.path:
    sys.path.append("/opt/airflow/plugins")

from hooks.football_api_hook import FootballApiHook

BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")

def fetch_and_store_fixtures():
    # 1) call the API
    api = FootballApiHook()
    blob = api.get_fixtures_today(league_id=os.getenv("FOOTBALL_LEAGUE_ID"))

    # 2) write raw JSON to bronze
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        s3.meta.client.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"bronze/fixtures/dt={dt}/fixtures_{ts}.json"

    s3.Object(BUCKET, key).put(Body=json.dumps(blob).encode("utf-8"))
    print("Wrote", f"s3://{BUCKET}/{key}")

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(seconds=10)}

with DAG(
    dag_id="fixtures_to_bronze",
    start_date=datetime(2025, 8, 19),
    schedule="@daily",   # you can trigger manually for now
    catchup=False,
    default_args=default_args,
    tags=["ingest","fixtures","bronze"],
) as dag:
    PythonOperator(task_id="fetch_and_store_fixtures", python_callable=fetch_and_store_fixtures)
