from datetime import datetime, timedelta, timezone
import os, io, json
import boto3
from botocore.config import Config
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET   = os.getenv("S3_BUCKET", "sports")
ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
REGION   = os.getenv("AWS_REGION", "us-east-1")
LEAGUE   = int(os.getenv("FOOTBALL_LEAGUE_ID", "629"))

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

def _read_latest_silver(season: int) -> pd.DataFrame:
    """
    Read the latest dt partition under:
      silver/fixtures/league={LEAGUE}/season={season}/dt=YYYY-MM-DD/fixtures.parquet
    """
    s3 = _s3()
    prefix = f"silver/fixtures/league={LEAGUE}/season={season}/"
    objs = list(s3.Bucket(BUCKET).objects.filter(Prefix=prefix))

    dts = sorted({seg.split("=", 1)[1] for o in objs for seg in o.key.split("/") if seg.startswith("dt=")})
    if not dts:
        raise RuntimeError(f"No silver partitions under s3://{BUCKET}/{prefix}")
    dt = dts[-1]
    key = f"{prefix}dt={dt}/fixtures.parquet"
    body = s3.Object(BUCKET, key).get()["Body"].read()
    return pd.read_parquet(io.BytesIO(body))

def _to_long_team_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Explode fixtures into team-centric rows (home + away perspectives)."""
    # Home perspective
    home = df[["fixture_id", "fixture_ts_utc", "home_team_id", "away_team_id", "goals_home", "goals_away"]].copy()
    home.rename(columns={
        "home_team_id": "team_id",
        "away_team_id": "opp_id",
        "goals_home": "gf",
        "goals_away": "ga",
    }, inplace=True)
    home["is_home"] = True

    # Away perspective
    away = df[["fixture_id", "fixture_ts_utc", "away_team_id", "home_team_id", "goals_home", "goals_away"]].copy()
    away.rename(columns={
        "away_team_id": "team_id",
        "home_team_id": "opp_id",
        "goals_away": "gf",
        "goals_home": "ga",
    }, inplace=True)
    away["is_home"] = False

    long_df = pd.concat([home, away], ignore_index=True)
    # W/D/L from team's POV
    res = long_df["gf"] - long_df["ga"]
    long_df["win_int"] = res.apply(lambda x: 1 if x > 0 else (0 if x == 0 else -1))

    # Sort and SHIFT to avoid leakage (only prior matches count)
    long_df = long_df.sort_values(["team_id", "fixture_ts_utc"]).reset_index(drop=True)
    long_df["prev_gf"] = long_df.groupby("team_id")["gf"].shift(1)
    long_df["prev_ga"] = long_df.groupby("team_id")["ga"].shift(1)
    long_df["prev_win_int"] = long_df.groupby("team_id")["win_int"].shift(1)
    return long_df

def _rolling_last5_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling-last-5 stats per team based on *previous* matches only."""
    def compute_per_team(grp: pd.DataFrame) -> pd.DataFrame:
        w = 5

        cnt = grp["prev_win_int"].rolling(w, min_periods=1).count()

        gf_sum = grp["prev_gf"].rolling(w, min_periods=1).sum()
        ga_sum = grp["prev_ga"].rolling(w, min_periods=1).sum()
        wins   = (grp["prev_win_int"] == 1).rolling(w, min_periods=1).sum()
        draws  = (grp["prev_win_int"] == 0).rolling(w, min_periods=1).sum()
        losses = (grp["prev_win_int"] == -1).rolling(w, min_periods=1).sum()
        pts    = ((grp["prev_win_int"] == 1) * 3 + (grp["prev_win_int"] == 0) * 1).rolling(w, min_periods=1).sum()

        gf_avg = gf_sum / cnt
        ga_avg = ga_sum / cnt
        ppg    = pts / cnt
        win_rt = wins / cnt

        out = pd.DataFrame({
            "team_id": grp["team_id"].values,
            "fixture_id": grp["fixture_id"].values,
            "fixture_ts_utc": grp["fixture_ts_utc"].values,
            "l5_gf_avg": gf_avg.values,
            "l5_ga_avg": ga_avg.values,
            "l5_points_avg": ppg.values,
            "l5_win_rate": win_rt.values,
            "l5_wins": wins.values,
            "l5_draws": draws.values,
            "l5_losses": losses.values,
            "l5_gf_sum": gf_sum.values,
            "l5_ga_sum": ga_sum.values,
        })

        win_seq = (grp["prev_win_int"] == 1).astype(int)

        win_groups = (win_seq == 0).cumsum()
        win_streak = win_seq.groupby(win_groups).cumcount() + 1
        win_streak[win_seq == 0] = 0

        unb_seq = (grp["prev_win_int"] >= 0).astype(int)
        unb_groups = (unb_seq == 0).cumsum()
        unb_streak = unb_seq.groupby(unb_groups).cumcount() + 1
        unb_streak[unb_seq == 0] = 0

        out["streak_wins"] = win_streak.values
        out["streak_unbeaten"] = unb_streak.values
        return out

    feats = long_df.groupby("team_id", group_keys=False).apply(compute_per_team)
    return feats.reset_index(drop=True)

def _labels_from_fixture_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    diff = df["goals_home"].astype("Int64") - df["goals_away"].astype("Int64")
    df["label_1x2"] = pd.Series(pd.cut(diff, bins=[-100, -0.5, 0.5, 100], labels=["2","X","1"])).astype("string")
    df["label_ou25"] = ((df["goals_home"].astype("Int64") + df["goals_away"].astype("Int64")) >= 3).astype(int)
    return df

def _build_features_for_season(season: int):
    """
    For a given season:
      1) read latest silver parquet
      2) compute rolling/streak features (home & away) *pre-match*
      3) create labels
      4) write features to s3://{BUCKET}/features/fixtures/league={LEAGUE}/season={season}/dt=YYYY-MM-DD/
    """
    s3 = _s3()
    df = _read_latest_silver(season)

    if "status_short" in df.columns:
        df = df[df["status_short"] == "FT"].copy()

    long_df = _to_long_team_rows(df)
    feats_long = _rolling_last5_features(long_df)

    home_feats = feats_long.merge(df[["fixture_id", "home_team_id"]], on="fixture_id", how="inner")
    home_feats = home_feats[home_feats["team_id"] == home_feats["home_team_id"]].copy()
    home_feats.drop(columns=["team_id"], inplace=True)
    home_feats = home_feats.add_prefix("home_")
    home_feats.rename(columns={"home_fixture_id": "fixture_id"}, inplace=True)  # undo prefix on key
    home_feats["fixture_id"] = home_feats["fixture_id"].astype("Int64")

    away_feats = feats_long.merge(df[["fixture_id", "away_team_id"]], on="fixture_id", how="inner")
    away_feats = away_feats[away_feats["team_id"] == away_feats["away_team_id"]].copy()
    away_feats.drop(columns=["team_id"], inplace=True)
    away_feats = away_feats.add_prefix("away_")
    away_feats.rename(columns={"away_fixture_id": "fixture_id"}, inplace=True)
    away_feats["fixture_id"] = away_feats["fixture_id"].astype("Int64")

    labeled = _labels_from_fixture_df(df)

    X = labeled.merge(home_feats, on="fixture_id", how="left").merge(away_feats, on="fixture_id", how="left")

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_prefix = f"features/fixtures/league={LEAGUE}/season={season}/dt={dt}/"
    s3.Bucket(BUCKET).put_object(Key=out_prefix)

    pbuf = io.BytesIO()
    X.to_parquet(pbuf, index=False); pbuf.seek(0)
    s3.Object(BUCKET, out_prefix + "features.parquet").put(Body=pbuf.getvalue())

    cbuf = io.StringIO()
    X.head(50).to_csv(cbuf, index=False)
    s3.Object(BUCKET, out_prefix + "features_sample.csv").put(Body=cbuf.getvalue().encode("utf-8"))

    checks = {
        "rows": int(len(X)),
        "cols": int(X.shape[1]),
        "label_1x2_counts": X["label_1x2"].value_counts(dropna=False).to_dict(),
        "ou25_rate": float(X["label_ou25"].mean()) if "label_ou25" in X else None,
        "some_home_cols": [c for c in X.columns if c.startswith("home_")][:8],
        "some_away_cols": [c for c in X.columns if c.startswith("away_")][:8],
    }
    s3.Object(BUCKET, out_prefix + "checks.json").put(Body=json.dumps(checks, default=str).encode("utf-8"))
    print(f"[features] season={season} dt={dt} wrote {len(X)} rows → s3://{BUCKET}/{out_prefix}")

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(seconds=15)}

with DAG(
    dag_id="silver_to_features",
    start_date=datetime(2025, 8, 19),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["features", "ml"],
) as dag:
    feat_2021 = PythonOperator(
        task_id="features_2021",
        python_callable=_build_features_for_season,
        op_kwargs={"season": 2021},
    )
    feat_2022 = PythonOperator(
        task_id="features_2022",
        python_callable=_build_features_for_season,
        op_kwargs={"season": 2022},
    )
    feat_2023 = PythonOperator(
        task_id="features_2023",
        python_callable=_build_features_for_season,
        op_kwargs={"season": 2023},
    )

    [feat_2021, feat_2022, feat_2023]