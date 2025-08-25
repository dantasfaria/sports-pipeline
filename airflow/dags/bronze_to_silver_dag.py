from datetime import datetime, timedelta, timezone
import os, io, json, re
import boto3
from botocore.config import Config
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

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
        config=Config(s3={"addressing_style": "path"}),
    )

DT_RE = re.compile(r"/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/")

def _latest_dt_under(prefix: str, s3):
    """Return the latest dt=YYYY-MM-DD partition under prefix (robust to depth)."""
    keys = [o.key for o in s3.Bucket(BUCKET).objects.filter(Prefix=prefix)]
    dts = []
    for k in keys:
        m = DT_RE.search("/" + k)  # ensure leading slash for regex consistency
        if m:
            dts.append(m.group(1))
    if not dts:
        return None
    return sorted(set(dts))[-1]

def bronze_season_to_silver(season: int, dt: str | None = None):
    """
    Read bronze raw for league=629, given season; flatten to silver parquet.
    Bronze path: s3://sports/bronze/fixtures/league=629/season=<season>/dt=<dt>/*.json
    Silver path: s3://sports/silver/fixtures/league=629/season=<season>/dt=<dt>/fixtures.parquet
    If dt not provided, use the latest dt= partition found under that season.
    """
    s3 = _s3()
    base_prefix = f"bronze/fixtures/league=629/season={season}/"

    if not dt:
        dt = _latest_dt_under(base_prefix, s3)
        if not dt:
            raise AirflowSkipException(f"No bronze under s3://{BUCKET}/{base_prefix}")

    bronze_prefix = f"{base_prefix}dt={dt}/"
    silver_prefix = f"silver/fixtures/league=629/season={season}/dt={dt}/"

    objs = list(s3.Bucket(BUCKET).objects.filter(Prefix=bronze_prefix))
    if not objs:
        raise AirflowSkipException(f"No bronze under s3://{BUCKET}/{bronze_prefix}")

    rows = []
    for o in objs:
        body = s3.Object(BUCKET, o.key).get()["Body"].read()
        try:
            payload = json.loads(body)
        except Exception:
            payload = json.loads(body.decode("utf-8", "ignore"))

        data = payload["data"] if isinstance(payload, dict) and "data" in payload else payload
        if isinstance(data, dict) and "response" in data:
            items = data["response"]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        for item in items:
            fixture = (item.get("fixture") or {})
            league  = (item.get("league") or {})
            teams   = (item.get("teams") or {})
            goals   = (item.get("goals") or {})
            score   = (item.get("score") or {})
            status  = (fixture.get("status") or {})
            venue   = (fixture.get("venue") or {})

            rows.append({
                "fixture_id"      : fixture.get("id"),
                "fixture_date_utc": fixture.get("date"),
                "league_id"       : league.get("id"),
                "league_name"     : league.get("name"),
                "league_season"   : league.get("season"),
                "home_team_id"    : (teams.get("home") or {}).get("id"),
                "home_team"       : (teams.get("home") or {}).get("name"),
                "away_team_id"    : (teams.get("away") or {}).get("id"),
                "away_team"       : (teams.get("away") or {}).get("name"),
                "status_short"    : status.get("short"),
                "status_long"     : status.get("long"),
                "status_elapsed"  : status.get("elapsed"),
                "goals_home"      : goals.get("home"),
                "goals_away"      : goals.get("away"),
                "score_ht_home"   : (score.get("halftime") or {}).get("home"),
                "score_ht_away"   : (score.get("halftime") or {}).get("away"),
                "venue_name"      : venue.get("name"),
                "venue_city"      : venue.get("city"),
            })

    if not rows:
        raise AirflowSkipException("Bronze files found, but no items parsed.")

    df = pd.DataFrame(rows)

    # typing / cleaning
    if "fixture_date_utc" in df.columns:
        df["fixture_ts_utc"] = pd.to_datetime(df["fixture_date_utc"], errors="coerce", utc=True)

    for col in ["fixture_id","league_id","league_season","home_team_id","away_team_id",
                "goals_home","goals_away","score_ht_home","score_ht_away","status_elapsed"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["league_name","home_team","away_team","status_short","status_long","venue_name","venue_city"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "fixture_id" in df.columns:
        df = df.drop_duplicates(subset=["fixture_id"], keep="last")

    # write parquet + sample
    s3.Bucket(BUCKET).put_object(Key=silver_prefix)
    parquet_key = silver_prefix + "fixtures.parquet"
    csv_key     = silver_prefix + "fixtures_sample.csv"

    pbuf = io.BytesIO()
    df.to_parquet(pbuf, index=False); pbuf.seek(0)
    s3.Object(BUCKET, parquet_key).put(Body=pbuf.getvalue())

    cbuf = io.StringIO()
    df.head(50).to_csv(cbuf, index=False)
    s3.Object(BUCKET, csv_key).put(Body=cbuf.getvalue().encode("utf-8"))

    print(f"[silver] league=629 season={season} dt={dt} rows={len(df)}")
    print(f"Wrote parquet -> s3://{BUCKET}/{parquet_key}")
    print(f"Wrote sample -> s3://{BUCKET}/{csv_key}")

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(seconds=15)}

with DAG(
    dag_id       = "bronze_to_silver",
    start_date   = datetime(2025, 8, 19),
    schedule     = None,
    catchup      = False,
    default_args = default_args,
    tags         = ["transform","fixtures","silver"],
) as dag:
    season_2021 = PythonOperator(
        task_id         = "bronze_to_silver_2021",
        python_callable = bronze_season_to_silver,
        op_args         = [2021]
    )
    season_2022 = PythonOperator(
        task_id         = "bronze_to_silver_2022",
        python_callable = bronze_season_to_silver,
        op_args         = [2022]
    )
    season_2023 = PythonOperator(
        task_id         = "bronze_to_silver_2023",
        python_callable = bronze_season_to_silver,
        op_args         = [2023]
    )

    [season_2021, season_2022, season_2023]
