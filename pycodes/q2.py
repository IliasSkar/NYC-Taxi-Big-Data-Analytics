import argparse
import json
import math
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType
)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType as DT

AM = ""
H = ""
L     = 4
HOURS = ""

NYC_LAT_MIN =  40.4774
NYC_LAT_MAX =  40.9176
NYC_LON_MIN = -74.2591
NYC_LON_MAX = -73.7004

R_EARTH = 6371.0

SCHEMA_2015 = StructType([
    StructField("VendorID",              IntegerType(),  True),
    StructField("tpep_pickup_datetime",  TimestampType(),True),
    StructField("tpep_dropoff_datetime", TimestampType(),True),
    StructField("passenger_count",       IntegerType(),  True),
    StructField("trip_distance",         DoubleType(),   True),
    StructField("pickup_longitude",      DoubleType(),   True),
    StructField("pickup_latitude",       DoubleType(),   True),
    StructField("RateCodeID",            IntegerType(),  True),
    StructField("store_and_fwd_flag",    StringType(),   True),
    StructField("dropoff_longitude",     DoubleType(),   True),
    StructField("dropoff_latitude",      DoubleType(),   True),
    StructField("payment_type",          IntegerType(),  True),
    StructField("fare_amount",           DoubleType(),   True),
    StructField("extra",                 DoubleType(),   True),
    StructField("mta_tax",               DoubleType(),   True),
    StructField("tip_amount",            DoubleType(),   True),
    StructField("tolls_amount",          DoubleType(),   True),
    StructField("improvement_surcharge", DoubleType(),   True),
    StructField("total_amount",          DoubleType(),   True),
])


def parse_args():
    parser = argparse.ArgumentParser(description="Q2 Congestion Analysis 2015")
    parser.add_argument("--parquet2015", required=True)
    parser.add_argument("--csv2015",     required=True)
    parser.add_argument("--output",      required=True)
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


def enrich(df):
    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast(TimestampType()))
    elif "pickup_ts" in df.columns:
        df = df.withColumn("pickup_ts", F.col("pickup_ts").cast(TimestampType()))

    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    elif "dropoff_ts" in df.columns:
        df = df.withColumn("dropoff_ts", F.col("dropoff_ts").cast(TimestampType()))

    df = df.withColumn("pickup_hour",      F.hour("pickup_ts"))
    df = df.withColumn("pickup_date",      F.to_date("pickup_ts"))

    df = df.withColumn("duration_minutes",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    )
    df = df.withColumn("duration_hours",   F.col("duration_minutes") / 60.0)
    df = df.withColumn("trip_distance_km", F.col("trip_distance") * 1.60934)
    df = df.withColumn("speed_kmh",
        F.when(F.col("duration_hours") > 0,
               F.col("trip_distance_km") / F.col("duration_hours"))
         .otherwise(None)
    )
    return df
def add_haversine_builtin(df):
    lat1 = F.radians(F.col("pickup_latitude"))
    lat2 = F.radians(F.col("dropoff_latitude"))
    lon1 = F.radians(F.col("pickup_longitude"))
    lon2 = F.radians(F.col("dropoff_longitude"))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    df = df.withColumn("haversine_km", F.lit(R_EARTH) * c)
    df = df.withColumn("distance_gap_km",
        F.when(F.col("haversine_km") > 0.2,
               F.col("trip_distance_km") - F.col("haversine_km"))
         .otherwise(None)
    )
    df = df.withColumn("detour_ratio",
        F.when((F.col("haversine_km") > 0.2) & (F.col("haversine_km") > 0),
               F.col("trip_distance_km") / F.col("haversine_km"))
         .otherwise(None)
    )
    return df


def add_congestion_flag(df):
    return df.withColumn("congestion_candidate",
        (F.col("speed_kmh") < 10) & (F.col("trip_distance_km") >= 1)
    )


def apply_filter(df):
    return df.filter(
        F.col("pickup_ts").isNotNull() &
        F.col("dropoff_ts").isNotNull() &
        F.col("duration_minutes").isNotNull() &
        (F.col("duration_minutes") > 0) &
        (F.col("trip_distance") > 0) &
        F.col("pickup_latitude").isNotNull() &
        F.col("pickup_longitude").isNotNull() &
        F.col("dropoff_latitude").isNotNull() &
        F.col("dropoff_longitude").isNotNull() &
        F.col("pickup_latitude").between(NYC_LAT_MIN, NYC_LAT_MAX) &
        F.col("pickup_longitude").between(NYC_LON_MIN, NYC_LON_MAX) &
        F.col("dropoff_latitude").between(NYC_LAT_MIN, NYC_LAT_MAX) &
        F.col("dropoff_longitude").between(NYC_LON_MIN, NYC_LON_MAX) &
        F.col("pickup_hour").isin(HOURS)
    )


def compute_hourly_metrics(df, label):
    result = (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.percentile_approx("duration_minutes", 0.5), 2).alias("median_duration_minutes"),
            F.round(F.percentile_approx("duration_minutes", 0.9), 2).alias("p90_duration_minutes"),
            F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            F.round(F.sum("trip_distance_km") / F.sum("duration_hours"), 2).alias("agg_speed_kmh"),
            F.round(F.avg("haversine_km"), 2).alias("avg_haversine_km"),
            F.round(F.avg("distance_gap_km"), 2).alias("avg_distance_gap_km"),
            F.round(F.avg("detour_ratio"), 2).alias("avg_detour_ratio"),
            F.round(
                F.sum(F.col("congestion_candidate").cast("int")) / F.count("*") * 100, 4
            ).alias("congestion_candidate_share"),
        )
        .orderBy("pickup_hour")
    )
    print(f"\n  [{label}] Metrics by pickup_hour:")
    result.show(truncate=False)
    return result


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Q2_CongestionAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    metrics = {}

    section("Personalization Parameters")
    print(f"  AM={AM}  H={H}  L={L}")
    print(f"  Hours  : {HOURS}")
    print(f"  NYC bounds lat: [{NYC_LAT_MIN}, {NYC_LAT_MAX}]  lon: [{NYC_LON_MIN}, {NYC_LON_MAX}]")
    print(f"  Congestion definition: speed_kmh < 10 AND trip_distance_km >= 1")

    section("Loading & Personalization of Parquet 2015")

    df_parq_raw = enrich(spark.read.parquet(args.parquet2015))
    df_parq_raw = add_haversine_builtin(df_parq_raw)
    df_parq_raw = add_congestion_flag(df_parq_raw)
    df_parq = apply_filter(df_parq_raw)
    df_parq.cache()

    cnt_parq = df_parq.count()
    print(f"  Parquet 2015 records (after filtering): {cnt_parq:,}")

    section("1. DataFrame API without UDF — Built-in Haversine (CSV and Parquet)")

    df_csv_raw = enrich(
        spark.read.option("header", "true").schema(SCHEMA_2015).csv(args.csv2015)
    )
    df_csv_raw = add_haversine_builtin(df_csv_raw)
    df_csv_raw = add_congestion_flag(df_csv_raw)
    df_csv = apply_filter(df_csv_raw)

    _, t_df_csv = timed("DataFrame CSV builtin",
        lambda: compute_hourly_metrics(df_csv, "DataFrame CSV builtin"))

    _, t_df_parq = timed("DataFrame Parquet builtin",
        lambda: compute_hourly_metrics(df_parq, "DataFrame Parquet builtin"))

    metrics["dataframe_builtin"] = {
        "csv_elapsed_sec":     t_df_csv,
        "parquet_elapsed_sec": t_df_parq,
    }

    section("2. DataFrame API with UDF — Python Haversine (Parquet)")

    def haversine_udf_fn(lat1, lon1, lat2, lon2):
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return None
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2)**2
        return R_EARTH * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    haversine_udf = udf(haversine_udf_fn, DT())

    def build_with_udf():
        df = enrich(spark.read.parquet(args.parquet2015))
        df = df.withColumn("haversine_km",
            haversine_udf(
                F.col("pickup_latitude"),  F.col("pickup_longitude"),
                F.col("dropoff_latitude"), F.col("dropoff_longitude")
            )
        )
        df = df.withColumn("distance_gap_km",
            F.when(F.col("haversine_km") > 0.2,
                   F.col("trip_distance_km") - F.col("haversine_km"))
             .otherwise(None)
        )
        df = df.withColumn("detour_ratio",
            F.when((F.col("haversine_km") > 0.2) & (F.col("haversine_km") > 0),
                   F.col("trip_distance_km") / F.col("haversine_km"))
             .otherwise(None)
        )
        df = add_congestion_flag(df)
        df = apply_filter(df)
        return compute_hourly_metrics(df, "DataFrame Parquet UDF")

    _, t_df_udf = timed("DataFrame Parquet UDF", build_with_udf)

    slowdown = round(t_df_udf / t_df_parq, 2) if t_df_parq > 0 else 0

    print(f"\n  Builtin Parquet: {t_df_parq}s  |  UDF Parquet: {t_df_udf}s  |  UDF slowdown: {slowdown}x")
    print(f"  Built-in: Catalyst + Tungsten optimization, vectorized execution.")
    print(f"  UDF: Python serialization overhead, cannot be optimized by Catalyst.")

    metrics["dataframe_udf"] = {
        "parquet_elapsed_sec": t_df_udf,
    }
    metrics["udf_vs_builtin"] = {
        "builtin_parquet_sec": t_df_parq,
        "udf_parquet_sec":     t_df_udf,
        "udf_slowdown_x":      slowdown,
    }

    section("3. Spark SQL — Parquet")

    df_parq.createOrReplaceTempView("trips_2015")

    def sql_hourly():
        res = spark.sql("""
            SELECT
                pickup_hour,
                COUNT(*) AS trips,
                ROUND(AVG(duration_minutes), 2)                                         AS avg_duration_minutes,
                ROUND(percentile_approx(duration_minutes, 0.5), 2)                     AS median_duration_minutes,
                ROUND(percentile_approx(duration_minutes, 0.9), 2)                     AS p90_duration_minutes,
                ROUND(AVG(speed_kmh), 2)                                                AS avg_speed_kmh,
                ROUND(SUM(trip_distance_km) / SUM(duration_hours), 2)                  AS agg_speed_kmh,
                ROUND(AVG(haversine_km), 2)                                             AS avg_haversine_km,
                ROUND(AVG(distance_gap_km), 2)                                          AS avg_distance_gap_km,
                ROUND(AVG(detour_ratio), 2)                                             AS avg_detour_ratio,
                ROUND(SUM(CAST(congestion_candidate AS INT)) / COUNT(*) * 100, 4)      AS congestion_candidate_share
            FROM trips_2015
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """)
        print("\n  [Spark SQL] Metrics by pickup_hour:")
        res.show(truncate=False)
        return res

    _, t_sql = timed("Spark SQL Parquet", sql_hourly)

    metrics["spark_sql"] = {
        "parquet_elapsed_sec": t_sql,
    }

    section("4. Top-5 slowest and Top-5 fastest trips")

    detail_cols = [
        "VendorID", "pickup_ts", "dropoff_ts", "trip_distance",
        "duration_minutes", "pickup_latitude", "pickup_longitude",
        "dropoff_latitude", "dropoff_longitude", "haversine_km",
        "trip_distance_km", "speed_kmh"
    ]

    top5_slow = (
        df_parq
        .withColumn("minutes_per_km", F.col("duration_minutes") / F.col("trip_distance_km"))
        .filter(F.col("trip_distance_km") > 0)
        .orderBy(F.desc("minutes_per_km"))
        .select(*detail_cols, "minutes_per_km")
        .limit(5)
    )
    print("\n  Top-5 slowest (longest duration per km):")
    top5_slow.show(truncate=False)

    top5_fast = (
        df_parq
        .filter(F.col("speed_kmh").isNotNull())
        .orderBy(F.desc("speed_kmh"))
        .select(*detail_cols)
        .limit(5)
    )
    print("\n  Top-5 fastest (highest speed):")
    top5_fast.show(truncate=False)

    section("Saving Results")

    out_hourly = f"{args.output}/q2_hourly_metrics_2015"
    (
        df_parq.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            F.round(F.avg("haversine_km"), 2).alias("avg_haversine_km"),
            F.round(
                F.sum(F.col("congestion_candidate").cast("int")) / F.count("*") * 100, 4
            ).alias("congestion_candidate_share"),
        )
        .orderBy("pickup_hour")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(out_hourly)
    )
    print(f"  Saved: {out_hourly}")

    out_slow = f"{args.output}/q2_top5_slow_2015"
    top5_slow.coalesce(1).write.mode("overwrite").parquet(out_slow)
    print(f"  Saved: {out_slow}")

    out_fast = f"{args.output}/q2_top5_fast_2015"
    top5_fast.coalesce(1).write.mode("overwrite").parquet(out_fast)
    print(f"  Saved: {out_fast}")

    metrics["personalization"] = {
        "AM": AM, "H": H, "L": L,
        "hours": HOURS,
        "count_filtered": cnt_parq,
        "nyc_bounds": {
            "lat": [NYC_LAT_MIN, NYC_LAT_MAX],
            "lon": [NYC_LON_MIN, NYC_LON_MAX],
        },
        "congestion_definition": "speed_kmh < 10 AND trip_distance_km >= 1",
    }

    metrics_path = f"{args.output}/q2_metrics"
    spark.sparkContext.parallelize(
        [json.dumps(metrics, indent=4, ensure_ascii=False)], 1
    ).saveAsTextFile(metrics_path)
    print(f"  Metrics: {metrics_path}")

    section("Q2 SUMMARY")
    print(f"  Personalization: hours {HOURS}")
    print(f"  Records after filtering: {cnt_parq:,}")
    print(f"  DataFrame builtin CSV:     {t_df_csv}s")
    print(f"  DataFrame builtin Parquet: {t_df_parq}s")
    print(f"  DataFrame UDF Parquet:     {t_df_udf}s  (slowdown {slowdown}x vs builtin)")
    print(f"  Spark SQL Parquet:         {t_sql}s")

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
