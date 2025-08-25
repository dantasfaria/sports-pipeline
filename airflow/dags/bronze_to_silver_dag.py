from datetime import datetime, timedelta, timezone
import os, io, json, sys
import boto3
from botocore.config import Config
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# --- Settings from env (Compose injects these) ---
BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")

# If someone keeps localhost in .env, make it work inside the container:
if ENDPOINT.startswith("http://localhost"):
    ENDPOINT = ENDPOINT.replace("localhost", "localstack")

def _s3():
    return boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=Config(s3={"addressing_style": "path"}),
    )

def bronze_fixtures_to_silver(**context):
    """
    Read today's bronze fixtures JSONs:
      s3://{BUCKET}/bronze/fixtures/dt=YYYY-MM-DD/fixtures_*.json
    Flatten into tabular columns and write:
      s3://{BUCKET}/silver/fixtures/dt=YYYY-MM-DD/fixtures.parquet
      (and fixtures_sample.csv for easy inspection)
    """
    s3 = _s3()
    dt = (context.get("dag_run") and context["dag_run"].conf.get("dt")) or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_prefix = f"bronze/fixtures/dt={dt}/"
    silver_prefix = f"silver/fixtures/dt={dt}/"

    # 1) List bronze files for today
    objs = list(s3.Bucket(BUCKET).objects.filter(Prefix=bronze_prefix))
    if not objs:
        raise AirflowSkipException(f"No bronze fixtures under s3://{BUCKET}/{bronze_prefix}")

    # 2) Load and normalize JSON
    rows = []
    for o in objs:
        body = s3.Object(BUCKET, o.key).get()["Body"].read()
        try:
            payload = json.loads(body)
        except Exception:
            # some runs return envelope {"endpoint":...,"data":{...}}
            # try to peel it:
            payload = json.loads(body.decode("utf-8", "ignore"))

        # API-Sports typically returns {"response": [ ... ]} possibly
        # wrapped in an envelope we created earlier as {"data": {...}}
        data = payload
        if isinstance(payload, dict) and "data" in payload:
            data = payload["data"]
        if isinstance(data, dict) and "response" in data:
            items = data["response"]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        for item in items:
            # Safely extract commonly-used fields
            fixture = (item.get("fixture") or {})
            league  = (item.get("league") or {})
            teams   = (item.get("teams") or {})
            goals   = (item.get("goals") or {})
            score   = (item.get("score") or {})
            status  = (fixture.get("status") or {})
            venue   = (fixture.get("venue") or {})

            rows.append({
                # Fixture
                "fixture_id"      : fixture.get("id"),
                "fixture_date_utc": fixture.get("date"),

                # League
                "league_id"       : league.get("id"),
                "league_name"     : league.get("name"),
                "league_season"   : league.get("season"),

                # Teams
                "home_team_id"    : (teams.get("home") or {}).get("id"),
                "home_team"       : (teams.get("home") or {}).get("name"),
                "away_team_id"    : (teams.get("away") or {}).get("id"),
                "away_team"       : (teams.get("away") or {}).get("name"),

                # Status
                "status_short"    : status.get("short"),
                "status_long"     : status.get("long"),
                "status_elapsed"  : status.get("elapsed"),

                # Goals
                "goals_home"      : goals.get("home"),
                "goals_away"      : goals.get("away"),

                # Score
                "score_ht_home"   : (score.get("halftime") or {}).get("home"),
                "score_ht_away"   : (score.get("halftime") or {}).get("away"),

                # Venue
                "venue_name"      : venue.get("name"),
                "venue_city"      : venue.get("city"),
            })

    if not rows:
        raise AirflowSkipException("Bronze files found, but no items parsed from payload(s).")

    df = pd.DataFrame(rows)

    # 3) Basic cleaning/types
    # Convert datetime strings to pandas datetime (UTC)
    if "fixture_date_utc" in df.columns:
        df["fixture_ts_utc"] = pd.to_datetime(df["fixture_date_utc"], errors="coerce", utc=True)

    # integers where appropriate (safe conversions)
    for col in ["fixture_id","league_id","league_season","home_team_id","away_team_id",
                "goals_home","goals_away","score_ht_home","score_ht_away","status_elapsed"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # strings trim (optional, cheap)
    for col in ["league_name","home_team","away_team","status_short","status_long","venue_name","venue_city"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # dedupe on fixture_id, keep last seen
    if "fixture_id" in df.columns:
        df = df.drop_duplicates(subset=["fixture_id"], keep="last")

    # 4) Write Parquet and a small CSV sample to S3 (silver)
    s3.Bucket(BUCKET).put_object(Key=silver_prefix)  # ensure prefix exists (noop)

    # Parquet
    parquet_key = silver_prefix + "fixtures.parquet"
    parquet_buf = io.BytesIO()
    df.to_parquet(parquet_buf, index=False)
    parquet_buf.seek(0)
    s3.Object(BUCKET, parquet_key).put(Body=parquet_buf.getvalue())

    # Small CSV sample (first 50 rows) for quick eyeballing
    csv_key = silver_prefix + "fixtures_sample.csv"
    csv_buf = io.StringIO()
    df.head(50).to_csv(csv_buf, index=False)
    s3.Object(BUCKET, csv_key).put(Body=csv_buf.getvalue().encode("utf-8"))

    print(f"[silver] dt={dt} rows={len(df)}")
    print(f"Wrote parquet → s3://{BUCKET}/{parquet_key}  (rows={len(df)})")
    print(f"Wrote sample csv → s3://{BUCKET}/{csv_key}")

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(seconds=15)}

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2025, 8, 19),
    schedule="@daily",   # runs daily; you can also trigger manually
    catchup=False,
    default_args=default_args,
    tags=["transform","fixtures","silver"],
) as dag:
    PythonOperator(
        task_id="fixtures_bronze_to_silver",
        python_callable=bronze_fixtures_to_silver,
    )
