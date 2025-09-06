# spark/jobs/silver_to_features.py
from pyspark.sql import SparkSession, functions as F, Window
from datetime import datetime, timezone
import argparse

def main(league: int, season: int, bucket: str):
    spark = (
        SparkSession.builder.appName("silver-to-features")
        .getOrCreate()
    )

    silver_glob = f"s3a://{bucket}/silver/fixtures/league={league}/season={season}/dt=*/fixtures.parquet"
    df = spark.read.parquet(silver_glob)
    df = df.withColumn("dt", F.regexp_extract(F.input_file_name(), r"dt=([0-9\-]+)", 1))
    max_dt = df.select(F.max("dt")).first()[0]
    df = df.filter(F.col("dt") == max_dt)

    if "status_short" in df.columns:
        df = df.filter(F.col("status_short") == F.lit("FT"))

    home = (
        df.select(
            "fixture_id", "fixture_ts_utc",
            F.col("home_team_id").alias("team_id"),
            F.col("away_team_id").alias("opp_id"),
            F.col("goals_home").cast("int").alias("gf"),
            F.col("goals_away").cast("int").alias("ga"),
        )
        .withColumn("is_home", F.lit(True))
    )

    away = (
        df.select(
            "fixture_id", "fixture_ts_utc",
            F.col("away_team_id").alias("team_id"),
            F.col("home_team_id").alias("opp_id"),
            F.col("goals_away").cast("int").alias("gf"),
            F.col("goals_home").cast("int").alias("ga"),
        )
        .withColumn("is_home", F.lit(False))
    )

    long_df = home.unionByName(away)
    long_df = long_df.withColumn(
        "win_int",
        F.when(F.col("gf") > F.col("ga"), 1)
         .when(F.col("gf") == F.col("ga"), 0)
         .otherwise(-1),
    )

    w_order  = Window.partitionBy("team_id").orderBy("fixture_ts_utc")
    w_prev5  = w_order.rowsBetween(-5, -1)

    cnt   = F.count("win_int").over(w_prev5)
    gf_sm = F.sum("gf").over(w_prev5)
    ga_sm = F.sum("ga").over(w_prev5)
    wins  = F.sum(F.when(F.col("win_int") == 1, 1).otherwise(0)).over(w_prev5)
    draws = F.sum(F.when(F.col("win_int") == 0, 1).otherwise(0)).over(w_prev5)
    loss  = F.sum(F.when(F.col("win_int") == -1, 1).otherwise(0)).over(w_prev5)
    pts   = F.sum(F.when(F.col("win_int") == 1, 3).when(F.col("win_int") == 0, 1).otherwise(0)).over(w_prev5)

    feats_long = (
        long_df
        .withColumn("l5_gf_sum", gf_sm)
        .withColumn("l5_ga_sum", ga_sm)
        .withColumn("l5_wins", wins)
        .withColumn("l5_draws", draws)
        .withColumn("l5_losses", loss)
        .withColumn("l5_points_avg", (pts / cnt))
        .withColumn("l5_gf_avg", (gf_sm / cnt))
        .withColumn("l5_ga_avg", (ga_sm / cnt))
        .withColumn("l5_win_rate", (wins / cnt))
    )

    is_win_prev = F.lag(F.when(F.col("win_int") == 1, 1).otherwise(0), 1).over(w_order)
    is_unb_prev = F.lag(F.when(F.col("win_int") >= 0, 1).otherwise(0), 1).over(w_order)

    feats_long = feats_long.withColumn("is_win_prev", is_win_prev).withColumn("is_unb_prev", is_unb_prev)

    grp_wins = F.sum(F.when(F.col("is_win_prev") == 0, 1).otherwise(0)).over(w_order)
    grp_unb  = F.sum(F.when(F.col("is_unb_prev") == 0, 1).otherwise(0)).over(w_order)

    feats_long = feats_long.withColumn("grp_wins", grp_wins).withColumn("grp_unb", grp_unb)

    w_grp_wins = Window.partitionBy("team_id", "grp_wins").orderBy("fixture_ts_utc")
    w_grp_unb  = Window.partitionBy("team_id", "grp_unb").orderBy("fixture_ts_utc")

    feats_long = (
        feats_long
        .withColumn("streak_wins", F.when(F.col("is_win_prev") == 1, F.row_number().over(w_grp_wins)).otherwise(F.lit(0)))
        .withColumn("streak_unbeaten", F.when(F.col("is_unb_prev") == 1, F.row_number().over(w_grp_unb)).otherwise(F.lit(0)))
        .drop("is_win_prev", "is_unb_prev", "grp_wins", "grp_unb")
    )

    base_cols = [
        "fixture_id","fixture_ts_utc","team_id",
        "l5_gf_avg","l5_ga_avg","l5_points_avg","l5_win_rate",
        "l5_wins","l5_draws","l5_losses","l5_gf_sum","l5_ga_sum",
        "streak_wins","streak_unbeaten"
    ]
    feats_long = feats_long.select(*base_cols)

    home_feats = (
        feats_long.join(df.select("fixture_id","home_team_id"), "fixture_id")
        .where(F.col("team_id") == F.col("home_team_id"))
        .drop("team_id","home_team_id")
    )
    for c in home_feats.columns:
        if c != "fixture_id":
            home_feats = home_feats.withColumnRenamed(c, f"home_{c}")

    away_feats = (
        feats_long.join(df.select("fixture_id","away_team_id"), "fixture_id")
        .where(F.col("team_id") == F.col("away_team_id"))
        .drop("team_id","away_team_id")
    )
    for c in away_feats.columns:
        if c != "fixture_id":
            away_feats = away_feats.withColumnRenamed(c, f"away_{c}")

    labeled = (
        df.select(
            "fixture_id","fixture_ts_utc","home_team_id","away_team_id",
            "goals_home","goals_away"
        )
        .withColumn(
            "label_1x2",
            F.when(F.col("goals_home") > F.col("goals_away"), F.lit("1"))
             .when(F.col("goals_home") == F.col("goals_away"), F.lit("X"))
             .otherwise(F.lit("2"))
        )
        .withColumn("label_ou25", (F.col("goals_home") + F.col("goals_away") >= 3).cast("int"))
    )

    X = labeled.join(home_feats, "fixture_id", "left").join(away_feats, "fixture_id", "left")

    dt_out = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_prefix = f"s3a://{bucket}/features/fixtures/league={league}/season={season}/dt={dt_out}"

    X.write.mode("overwrite").parquet(out_prefix + "/features.parquet")
    X.limit(50).coalesce(1).write.mode("overwrite").option("header", True).csv(out_prefix + "/features_sample.csv")

    print(f"[features] league={league} season={season} dt={dt_out} rows={X.count()} -> {out_prefix}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--league", type=int, required=True)
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--bucket", type=str, default="sports")
    args = p.parse_args()
    main(args.league, args.season, args.bucket)