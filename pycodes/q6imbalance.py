import argparse
import json
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

AM = ""
H = ""
L     = 4
HOURS = ""
DAYS = ""
K = ""

ACTIVITY_THRESHOLD = 30


def parse_args():
    parser = argparse.ArgumentParser(description="Q6 — Zone Pickup/Dropoff Imbalance 2024")
    parser.add_argument("--parquet2024", required=True, help="Input location for yellow_tripdata_2024")
    parser.add_argument("--zones",       required=True, help="Input location for taxi_zone_lookup (parquet)")
    parser.add_argument("--output",      required=True, help="Output location")
    return parser.parse_args()


def section(title):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def timed(label, action):
    t0 = time.time()
    result = action()
    elapsed = round(time.time() - t0, 2)
    print(f"  [timer] {label}: {elapsed}s")
    return result, elapsed


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Q6_ZoneImbalance")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    section("Personalization Parameters")
    print(f"  AM={AM}  H={H}  L={L}")
    print(f"  Hours           : {HOURS}")
    print(f"  Days            : {DAYS}")
    print(f"  K              : {K}")
    print(f"  Activity >= {ACTIVITY_THRESHOLD}")

    raw = spark.read.parquet(args.parquet2024)

    df = (
        raw
        .withColumnRenamed("VendorID",     "vendor_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumnRenamed("Airport_fee",  "airport_fee")
        .withColumn(
            "dropoff_ts",
            F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        )
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
        )
    )

    df = df.filter(
        F.col("pickup_day").isin(DAYS)
        & F.col("pickup_hour").isin(HOURS)
        & (F.year("pickup_ts") == 2024)
        & F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("duration_minutes") > 0)
        & (F.col("total_amount")     > 0)
    )

    df.cache()
    cnt = df.count()
    print(f"\n  Records after filtering: {cnt:,}")

    section("1. DataFrame API — Pickup/Dropoff Imbalance")

    def df_imbalance():

        pickups = (
            df
            .groupBy(
                F.col("pickup_hour"),
                F.col("pu_location_id").alias("location_id")
            )
            .agg(F.count("*").alias("pickups"))
        )

        dropoffs = (
            df
            .groupBy(
                F.col("pickup_hour"),
                F.col("do_location_id").alias("location_id")
            )
            .agg(F.count("*").alias("dropoffs"))
        )

        imbalance = (
            pickups
            .join(dropoffs, on=["pickup_hour", "location_id"], how="full")
            .fillna(0, subset=["pickups", "dropoffs"])
            .withColumn("activity",
                F.col("pickups") + F.col("dropoffs")
            )
            .withColumn("net_pickups",
                F.col("pickups") - F.col("dropoffs")
            )
            .withColumn("imbalance_ratio",
                F.when(
                    F.col("activity") > 0,
                    F.round(
                        (F.col("pickups") - F.col("dropoffs")) / F.col("activity"),
                        4
                    )
                ).otherwise(None)
            )
            .withColumn("abs_imbalance_ratio",
                F.abs(F.col("imbalance_ratio"))
            )
        )

        zones = spark.read.parquet(args.zones)

        imbalance = (
            imbalance
            .join(
                F.broadcast(
                    zones.select("location_id", "borough", "zone", "service_zone")
                ),
                on="location_id",
                how="left"
            )
        )

        imbalance_filtered = imbalance.filter(F.col("activity") >= ACTIVITY_THRESHOLD)
        imbalance_filtered.cache()

        top_positive = (
            imbalance_filtered
            .orderBy(F.desc("net_pickups"))
            .limit(K)
        )
        print(f"\n  [DataFrame API] Top-{K} zones with the highest positive net_pickups:")
        top_positive.select(
            "pickup_hour", "location_id", "zone", "borough",
            "pickups", "dropoffs", "activity", "net_pickups",
            "imbalance_ratio", "abs_imbalance_ratio"
        ).show(K, truncate=False)

        top_negative = (
            imbalance_filtered
            .orderBy(F.asc("net_pickups"))
            .limit(K)
        )
        print(f"\n  [DataFrame API] Top-{K} zones with the most negative net_pickups:")
        top_negative.select(
            "pickup_hour", "location_id", "zone", "borough",
            "pickups", "dropoffs", "activity", "net_pickups",
            "imbalance_ratio", "abs_imbalance_ratio"
        ).show(K, truncate=False)

        top_abs = (
            imbalance_filtered
            .orderBy(F.desc("abs_imbalance_ratio"))
            .limit(K)
        )
        print(f"\n  [DataFrame API] Top-{K} zones with the highest abs_imbalance_ratio (activity >= {ACTIVITY_THRESHOLD}):")
        top_abs.select(
            "pickup_hour", "location_id", "zone", "borough",
            "pickups", "dropoffs", "activity", "net_pickups",
            "imbalance_ratio", "abs_imbalance_ratio"
        ).show(K, truncate=False)

        hourly_summary = (
            imbalance_filtered
            .groupBy("pickup_hour")
            .agg(
                F.sum(F.abs(F.col("net_pickups"))).alias("total_abs_net_pickups"),
                F.round(F.avg("abs_imbalance_ratio"), 4).alias("avg_abs_imbalance_ratio"),
                F.count("location_id").alias("zones_above_threshold"),
            )
            .orderBy("pickup_hour")
        )
        print(f"\n  [DataFrame API] Imbalance summary by pickup_hour:")
        hourly_summary.show(truncate=False)

        return imbalance_filtered, top_positive, top_negative, top_abs, hourly_summary

    (imbalance_filtered, top_positive, top_negative, top_abs, hourly_summary), t_df = timed(
        "DataFrame API Imbalance", df_imbalance
    )

    section("2. Spark SQL — Equivalent Implementation with CTEs")

    df.createOrReplaceTempView("trips_q6")

    zones_spark = spark.read.parquet(args.zones)
    zones_spark.createOrReplaceTempView("zones_lookup")

    def sql_imbalance():
        result = spark.sql(f"""
            WITH pickups AS (
                SELECT
                    pickup_hour,
                    pu_location_id AS location_id,
                    COUNT(*) AS pickups
                FROM trips_q6
                GROUP BY pickup_hour, pu_location_id
            ),
            dropoffs AS (
                -- pickup_hour as the common time reference (not dropoff_hour)
                SELECT
                    pickup_hour,
                    do_location_id AS location_id,
                    COUNT(*) AS dropoffs
                FROM trips_q6
                GROUP BY pickup_hour, do_location_id
            ),
            joined AS (
                SELECT
                    COALESCE(p.pickup_hour, d.pickup_hour)   AS pickup_hour,
                    COALESCE(p.location_id, d.location_id)   AS location_id,
                    COALESCE(p.pickups,  0)                  AS pickups,
                    COALESCE(d.dropoffs, 0)                  AS dropoffs
                FROM pickups p
                FULL OUTER JOIN dropoffs d
                    ON p.pickup_hour = d.pickup_hour
                    AND p.location_id = d.location_id
            ),
            metrics AS (
                SELECT
                    pickup_hour,
                    location_id,
                    pickups,
                    dropoffs,
                    (pickups + dropoffs)                             AS activity,
                    (pickups - dropoffs)                             AS net_pickups,
                    CASE
                        WHEN (pickups + dropoffs) > 0
                        THEN ROUND(
                            (pickups - dropoffs) / (pickups + dropoffs), 4
                        )
                        ELSE NULL
                    END                                              AS imbalance_ratio,
                    CASE
                        WHEN (pickups + dropoffs) > 0
                        THEN ROUND(
                            ABS(pickups - dropoffs) / (pickups + dropoffs), 4
                        )
                        ELSE NULL
                    END                                              AS abs_imbalance_ratio
                FROM joined
                WHERE (pickups + dropoffs) >= {ACTIVITY_THRESHOLD}
            )
            SELECT
                m.*,
                z.borough,
                z.zone,
                z.service_zone
            FROM metrics m
            LEFT JOIN zones_lookup z ON m.location_id = z.location_id
            ORDER BY net_pickups DESC
        """)
        print(f"\n  [Spark SQL] Top-{K} positive net_pickups:")
        result.limit(K).show(K, truncate=False)

        print(f"\n  [Spark SQL] Top-{K} negative net_pickups:")
        result.orderBy(F.asc("net_pickups")).limit(K).show(K, truncate=False)

        print(f"\n  [Spark SQL] Top-{K} abs_imbalance_ratio:")
        result.orderBy(F.desc("abs_imbalance_ratio")).limit(K).show(K, truncate=False)

        return result

    sql_result, t_sql = timed("Spark SQL Imbalance", sql_imbalance)

    section("Saving Results")

    paths = {
        "top_positive":    f"{args.output}/q6_top_positive_net",
        "top_negative":    f"{args.output}/q6_top_negative_net",
        "top_abs":         f"{args.output}/q6_top_abs_imbalance",
        "hourly_summary":  f"{args.output}/q6_hourly_summary",
        "sql_result":      f"{args.output}/q6_sql_result",
    }

    top_positive.coalesce(1).write.mode("overwrite").parquet(paths["top_positive"])
    top_negative.coalesce(1).write.mode("overwrite").parquet(paths["top_negative"])
    top_abs.coalesce(1).write.mode("overwrite").parquet(paths["top_abs"])
    hourly_summary.coalesce(1).write.mode("overwrite").parquet(paths["hourly_summary"])
    sql_result.coalesce(1).write.mode("overwrite").parquet(paths["sql_result"])

    for name, path in paths.items():
        print(f"  Saved [{name}]: {path}")

    metrics = {
        "personalization": {
            "AM": AM, "H": H, "L": L,
            "hours": HOURS, "days": DAYS, "K": K,
            "activity_threshold": ACTIVITY_THRESHOLD,
        },
        "count_filtered": cnt,
        "timings": {
            "df_imbalance_sec":  t_df,
            "sql_imbalance_sec": t_sql,
        },
    }

    spark.createDataFrame(
        [(json.dumps(metrics, indent=4, ensure_ascii=False),)], ["value"]
    ).coalesce(1).write.mode("overwrite").text(f"{args.output}/q6_metrics")
    print(f"  Metrics: {args.output}/q6_metrics")

    section("Q6 SUMMARY — Imbalance")
    print(f"  Hours / Days       : {HOURS} / {DAYS}")
    print(f"  Filtered records   : {cnt:,}")
    print(f"  Activity threshold  : >= {ACTIVITY_THRESHOLD}")
    print(f"  DataFrame API       : {t_df}s")
    print(f"  Spark SQL           : {t_sql}s")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
