import os, io, json
import boto3
from botocore.config import Config
import pandas as pd

BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")
LEAGUE   = 629
SEASONS  = [2021, 2022, 2023]

def s3():
    return boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=Config(s3={"addressing_style":"path"}),
    )

def read_silver_parquet(season: int) -> pd.DataFrame:
    r = s3()
    base = f"silver/fixtures/league={LEAGUE}/season={season}/"
    objs = list(r.Bucket(BUCKET).objects.filter(Prefix=base))

    dts = set()
    for o in objs:
        parts = o.key.split("/")
        for seg in parts:
            if seg.startswith("dt="):
                dts.add(seg.split("=", 1)[1])
                break

    if not dts:
        raise RuntimeError(f"No silver dt partitions under s3://{BUCKET}/{base}")

    dt = sorted(dts)[-1]
    key = f"{base}dt={dt}/fixtures.parquet"

    try:
        body = r.Object(BUCKET, key).get()["Body"].read()
    except Exception as e:
        raise RuntimeError(
            f"Could not read {key}. Available keys:\n" +
            "\n".join(k.key for k in objs)
        ) from e

    return pd.read_parquet(io.BytesIO(body))

def quick_stats(df: pd.DataFrame, title: str):
    print(f"\n===== {title} =====")
    print("rows:", len(df), "unique fixtures:", df["fixture_id"].nunique())

    need = ["fixture_id","fixture_ts_utc","league_season","home_team_id","away_team_id","goals_home","goals_away","status_short"]
    missing = [c for c in need if c not in df.columns]

    if missing: print("MISSING columns:", missing)
    print(df[["league_season","status_short"]].value_counts().head(10))
    print("goals_home describe:\n", df["goals_home"].describe())
    print("goals_away describe:\n", df["goals_away"].describe())
    print("null %:\n", (df[need].isna().mean()*100).round(2))
    print("sample rows:\n", df.head(3))


def read_features_and_checks():
    r = s3()

    base = f"features/league={LEAGUE}/season=2023/"
    objs = list(r.Bucket(BUCKET).objects.filter(Prefix=base))
    dts = sorted({o.key.split("/")[4].split("=")[1] for o in objs if "/dt=" in o.key})

    if not dts:
        print("\n(no features found yet)")
        return
    
    dt = dts[-1]
    feat_key = f"{base}dt={dt}/features.parquet"
    chk_key  = f"{base}dt={dt}/checks.json"
    print(f"\n→ reading s3://{BUCKET}/{feat_key}")

    body = r.Object(BUCKET, feat_key).get()["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))
    print("features rows:", len(df), "columns:", len(df.columns))

    if "label_1x2" in df.columns:
        print("label_1x2 distribution:\n", df["label_1x2"].value_counts(dropna=False))
    try:
        c = r.Object(BUCKET, chk_key).get()["Body"].read()
        print("\nchecks.json:\n", json.dumps(json.loads(c), indent=2))
    except Exception:
        pass

    print("sample feature cols:\n", [c for c in df.columns if c.startswith("home_roll5_")][:6],
          " ... ", [c for c in df.columns if c.startswith("away_roll5_")][:6])
    
if __name__ == "__main__":
    for season in SEASONS:
        df = read_silver_parquet(season)
        quick_stats(df, f"silver season={season}")
    read_features_and_checks()