# scripts/qa_features.py
import os, io, json
import boto3
from botocore.config import Config
import pandas as pd

BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")
LEAGUE   = int(os.getenv("FOOTBALL_LEAGUE_ID", "629"))
SEASONS  = [2021, 2022, 2023]

def s3_resource():
    return boto3.resource(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"),
        region_name=REGION,
        endpoint_url=ENDPOINT,
        config=Config(s3={"addressing_style": "path"}),
    )

def read_latest_features(season: int) -> tuple[pd.DataFrame, str]:
    """
    Lê o último dt em:
      features/fixtures/league={LEAGUE}/season={season}/dt=YYYY-MM-DD/features.parquet
    Retorna (df, dt)
    """
    s3 = s3_resource()
    base = f"features/fixtures/league={LEAGUE}/season={season}/"
    objs = list(s3.Bucket(BUCKET).objects.filter(Prefix=base))

    dts = sorted({seg.split("=", 1)[1] for o in objs for seg in o.key.split("/") if seg.startswith("dt=")})
    if not dts:
        raise RuntimeError(f"Nenhum dt encontrado em s3://{BUCKET}/{base}")
    dt = dts[-1]

    feat_key = f"{base}dt={dt}/features.parquet"
    body = s3.Object(BUCKET, feat_key).get()["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))
    return df, dt

def read_checks_json(season: int, dt: str) -> dict | None:
    s3 = s3_resource()
    key = f"features/fixtures/league={LEAGUE}/season={season}/dt={dt}/checks.json"
    try:
        raw = s3.Object(BUCKET, key).get()["Body"].read()
        return json.loads(raw)
    except Exception:
        return None

def sanity_ranges(df: pd.DataFrame) -> dict:
    """
    Regras simples para conferir se as features estão em faixas plausíveis.
    """
    out = {}

    for col in ["home_l5_points_avg", "away_l5_points_avg"]:
        if col in df.columns:
            out[f"range_{col}"] = float(df[col].min()), float(df[col].max())
    for col in ["home_l5_win_rate", "away_l5_win_rate"]:
        if col in df.columns:
            out[f"range_{col}"] = float(df[col].min()), float(df[col].max())
    return out

def null_report(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return pd.Series(dtype=float)
    return (df[cols].isna().mean() * 100).round(2)

def qa_one_season(season: int):
    df, dt = read_latest_features(season)

    print(f"\n===== features season={season} (dt={dt}) =====")
    print("shape:", df.shape)

    dup = int(df["fixture_id"].duplicated().sum()) if "fixture_id" in df.columns else None
    if dup is not None:
        print("duplicated fixture_id:", dup)

    if "label_1x2" in df.columns:
        print("\nlabel_1x2 distribution:")
        print(df["label_1x2"].value_counts(dropna=False))
    if "label_ou25" in df.columns:
        print("\nlabel_ou25 rate:", float(df["label_ou25"].mean()))

    ranges = sanity_ranges(df)
    for k, (lo, hi) in ranges.items():
        print(f"{k} min/max: {lo:.3f} .. {hi:.3f}")

    key_cols = [
        "fixture_id", "fixture_ts_utc", "home_team_id", "away_team_id",
        "home_l5_gf_avg", "home_l5_ga_avg", "home_l5_points_avg", "home_l5_win_rate",
        "away_l5_gf_avg", "away_l5_ga_avg", "away_l5_points_avg", "away_l5_win_rate",
        "streak_wins", "streak_unbeaten",
    ]

    also_try = []
    for base in ["streak_wins", "streak_unbeaten"]:
        for side in ["home_", "away_"]:
            also_try.append(side + base)
    key_cols = list(dict.fromkeys(key_cols + also_try))

    print("\nnull % principais cols:")
    print(null_report(df, key_cols))

    show_cols = [c for c in df.columns if c.startswith("home_l5_")][:6] + \
                [c for c in df.columns if c.startswith("away_l5_")][:6] + \
                ["label_1x2", "label_ou25"]
    show_cols = [c for c in show_cols if c in df.columns]
    print("\namostra de colunas:")
    print(df[show_cols].head(5))

    checks = read_checks_json(season, dt)
    if checks:
        print("\nchecks.json:", json.dumps(checks, indent=2))

if __name__ == "__main__":
    for season in SEASONS:
        qa_one_season(season)