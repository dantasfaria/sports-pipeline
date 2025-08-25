import os, sys, json, boto3
from airflow import DAG
from botocore.config import Config
from datetime import datetime, timezone, timedelta
from airflow.operators.python import PythonOperator

if "/opt/airflow/plugins" not in sys.path:
    sys.path.append("/opt/airflow/plugins")
from hooks.football_api_hook import FootballApiHook

BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")

if ENDPOINT.startswith("http://localhost"):
    ENDPOINT = ENDPOINT.replace("localhost", "localstack")

def _s3():
    return boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=Config(s3={"addressing_style": "path"})
)

def _write_json_to_s3(prefix: str, dt: str, base_name: str, payload: dict):
    s3 = _s3()

    try:
        s3.meta.client.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
    
    ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"{prefix}/dt={dt}/{base_name}_{ts}.json"
    s3.Object(BUCKET, key).put(Body=json.dumps(payload).encode("utf-8"))
    print("Wrote", f"s3://{BUCKET}/{key}")

def fetch_championship_season_to_bronze(season: int):
    api      = FootballApiHook()
    blob     = api.get_fixtures(league = 629, season = season, status = "FT")
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    _write_json_to_s3(
        prefix    = f"bronze/fixtures/league=629/season={season}",
        dt        = dt,
        base_name = "fixtures_snapshot",
        payload   = blob
    )

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(seconds=10)}

with DAG(
    dag_id       = "fixtures_to_bronze",
    start_date   = datetime(2025, 8, 19),
    schedule     = None,
    catchup      = False,
    default_args = default_args,
    tags         = ["ingest","fixtures","bronze"],
) as dag:
    season_2021 = PythonOperator(
        task_id         = "fetch_championship_2021_to_bronze",
        python_callable = fetch_championship_season_to_bronze,
        op_args         = [2021]
    )
    season_2022 = PythonOperator(
        task_id         = "fetch_championship_2022_to_bronze",
        python_callable = fetch_championship_season_to_bronze,
        op_args         = [2022]
    )
    season_2023 = PythonOperator(
        task_id         = "fetch_championship_2023_to_bronze",
        python_callable = fetch_championship_season_to_bronze,
        op_args         = [2023]
    )

    [season_2021, season_2022, season_2023]
