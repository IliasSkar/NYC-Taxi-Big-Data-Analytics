import argparse
import json
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType, LongType
)

AM = ""
H = ""
L     = 4
HOURS = ""
DAYS = ""
K = ""

MIN_SUPPORT_FIXED   = 50
MIN_SUPPORT_PCT     = 0.01
MIN_SUPPORT_FLOOR   = 10

SCHEMA_2024 = StructType([
    StructField("VendorID",              IntegerType(),  True),
    StructField("tpep_pickup_datetime",  StringType(),   True),
    StructField("tpep_dropoff_datetime", StringType(),   True),
    StructField("passenger_count",       DoubleType(),   True),
    StructField("trip_distance",         DoubleType(),   True),
    StructField("RatecodeID",            DoubleType(),   True),
    StructField("store_and_fwd_flag",    StringType(),   True),
    StructField("PULocationID",          IntegerType(),  True),
    StructField("DOLocationID",          IntegerType(),  True),
    StructField("payment_type",          LongType(),     True),
    StructField("fare_amount",           DoubleType(),   True),
    StructField("extra",                 DoubleType(),   True),
    StructField("mta_tax",               DoubleType(),   True),
    StructField("tip_amount",            DoubleType(),   True),
    StructField("tolls_amount",          DoubleType(),   True),
    StructField("improvement_surcharge", DoubleType(),   True),
    StructField("total_amount",          DoubleType(),   True),
    StructField("congestion_surcharge",  DoubleType(),   True),
    StructField("Airport_fee",           DoubleType(),   True),
])

SCHEMA_ZONE = StructType([
    StructField("LocationID",    IntegerType(), True),
    StructField("Borough",       StringType(),  True),
    StructField("Zone",          StringType(),  True),
    StructField("service_zone",  StringType(),  True),
])


def parse_args():
    parser = argparse.ArgumentParser(description="Q3 Revenue & Zone Analysis 2024")
    parser.add_argument("--parquet2024",  required=True)
    parser.add_argument("--csv2024",      required=True)
    parser.add_argument("--zone_lookup",  required=True)
    parser.add_argument("--output",       required=True)
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


def load_and_enrich_csv(spark, path):
    df = spark.read.option("header", "true").schema(SCHEMA_2024).csv(path)
    return enrich(df)


def enrich(df):
    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("pickup_ts",
            F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    else:
        df = df.withColumn("pickup_ts", F.col("pickup_ts").cast(TimestampType()))

    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("dropoff_ts",
            F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    else:
        if "tpep_dropoff_datetime" not in df.columns and "dropoff_ts" in df.columns:
            df = df.withColumn("dropoff_ts", F.col("dropoff_ts").cast(TimestampType()))

    if "PULocationID" in df.columns: df = df.withColumnRenamed("PULocationID", "pu_location_id")
    if "DOLocationID" in df.columns: df = df.withColumnRenamed("DOLocationID", "do_location_id")
    if "Airport_fee" in df.columns:  df = df.withColumnRenamed("Airport_fee",  "airport_fee")

    if "pickup_hour" not in df.columns:
        df = df.withColumn("pickup_hour", F.hour("pickup_ts"))
    if "pickup_day" not in df.columns:
        df = df.withColumn("pickup_day",  F.dayofmonth("pickup_ts"))

    df = df.withColumn("duration_minutes",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0)

    for col_name in ["congestion_surcharge", "airport_fee", "extra", "mta_tax", "tolls_amount", "improvement_surcharge"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))
        else:
            df = df.withColumn(col_name, F.lit(0.0))

    return df


def apply_filter(df):
    return df.filter(
        (F.year("pickup_ts") == 2024) &
        F.col("pickup_ts").isNotNull() &
        F.col("dropoff_ts").isNotNull() &
        (F.col("duration_minutes") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        F.col("pickup_hour").isin(HOURS) &
        F.col("pickup_day").isin(DAYS)
    )


def apply_filter_no_pruning(df):
    return df.filter(
        (F.year("pickup_ts") == 2024) &
        F.col("pickup_ts").isNotNull() &
        F.col("dropoff_ts").isNotNull() &
        (F.col("duration_minutes") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        F.col("pickup_hour").isin(HOURS)
    )


def add_distance_bucket(df):
    return df.withColumn("distance_bucket",
        F.when(F.col("trip_distance") < 1,  "very_short")
         .when(F.col("trip_distance") < 3,  "short")
         .when(F.col("trip_distance") < 10, "medium")
         .otherwise("long")
    )


def join_zones(df, zone_df):
    pu_zone = zone_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    return df.join(F.broadcast(pu_zone), on="pu_location_id", how="left")


def compute_min_support(total_trips):
    pct_based = total_trips * MIN_SUPPORT_PCT
    support = min(MIN_SUPPORT_FIXED, pct_based)
    return max(support, MIN_SUPPORT_FLOOR)


def compute_metrics(df):
    return (
        df.groupBy("pickup_borough", "pickup_zone", "distance_bucket")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
            F.round(F.sum("total_amount") / F.sum("trip_distance"), 2).alias("revenue_per_mile"),
            F.round(F.sum("total_amount") / F.sum("duration_minutes"), 2).alias("revenue_per_minute"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(
                (F.sum("extra") + F.sum("mta_tax") + F.sum("tolls_amount") +
                 F.sum("improvement_surcharge") + F.sum("congestion_surcharge") +
                 F.sum("airport_fee")) / F.sum("total_amount"), 4
            ).alias("surcharge_share"),
        )
    )


def top_k_by(metrics_df, min_support, order_col, label):
    filtered = metrics_df.filter(F.col("trips") >= min_support)
    result = (
        filtered
        .orderBy(F.desc(order_col))
        .limit(K)
    )
    print(f"\n  Top-{K} zones by {label} (min_support={int(min_support)}):")
    result.show(K, truncate=False)
    return result


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Q3_RevenueZoneAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    section("Personalization Parameters")
    print(f"  AM={AM}  H={H}  L={L}")
    print(f"  Hours  : {HOURS}")
    print(f"  Days   : {DAYS}")
    print(f"  K={K}")
    print(f"  Min support: min({MIN_SUPPORT_FIXED}, 1% * total_trips), floor={MIN_SUPPORT_FLOOR}")

    zone_df = spark.read.option("header", "true").schema(SCHEMA_ZONE).csv(args.zone_lookup)
    zone_df.cache()

    section("1. DataFrame API — CSV and Parquet")

    df_csv_raw  = load_and_enrich_csv(spark, args.csv2024)
    df_csv      = apply_filter(df_csv_raw)
    df_csv      = add_distance_bucket(df_csv)
    df_csv      = join_zones(df_csv, zone_df)

    df_parq_raw = enrich(spark.read.parquet(args.parquet2024))
    df_parq     = apply_filter(df_parq_raw)
    df_parq     = add_distance_bucket(df_parq)
    df_parq     = join_zones(df_parq, zone_df)
    df_parq.cache()

    total_trips = df_parq.count()
    min_support = compute_min_support(total_trips)
    print(f"\n  Parquet 2024 records (after filtering): {total_trips:,}")
    print(f"  Applied min_support: {int(min_support)}")

    def run_csv_metrics():
        m = compute_metrics(df_csv)
        m.cache()
        top_k_by(m, min_support, "total_revenue",     "total_revenue")
        top_k_by(m, min_support, "revenue_per_mile",  "revenue_per_mile")
        top_k_by(m, min_support, "revenue_per_minute","revenue_per_minute")
        return m

    _, t_csv = timed("DataFrame CSV", run_csv_metrics)

    def run_parq_metrics():
        m = compute_metrics(df_parq)
        m.cache()
        top_k_by(m, min_support, "total_revenue",     "total_revenue")
        top_k_by(m, min_support, "revenue_per_mile",  "revenue_per_mile")
        top_k_by(m, min_support, "revenue_per_minute","revenue_per_minute")
        return m

    metrics_parq, t_parq = timed("DataFrame Parquet", run_parq_metrics)

    section("2. Spark SQL — Parquet")

    df_parq.createOrReplaceTempView("trips_2024")

    def sql_metrics():
        res = spark.sql(f"""
            SELECT
                pickup_borough,
                pickup_zone,
                distance_bucket,
                COUNT(*) AS trips,
                ROUND(SUM(total_amount), 2)                                                    AS total_revenue,
                ROUND(AVG(total_amount), 2)                                                    AS avg_revenue_per_trip,
                ROUND(SUM(total_amount) / SUM(trip_distance), 2)                              AS revenue_per_mile,
                ROUND(SUM(total_amount) / SUM(duration_minutes), 2)                           AS revenue_per_minute,
                ROUND(AVG(fare_amount), 2)                                                     AS avg_fare_amount,
                ROUND(AVG(tip_amount), 2)                                                      AS avg_tip_amount,
                ROUND(
                    (SUM(extra) + SUM(mta_tax) + SUM(tolls_amount) +
                     SUM(improvement_surcharge) + SUM(congestion_surcharge) +
                     SUM(airport_fee)) / SUM(total_amount), 4
                )                                                                              AS surcharge_share
            FROM trips_2024
            GROUP BY pickup_borough, pickup_zone, distance_bucket
        """)
        res.createOrReplaceTempView("metrics_2024")

        print(f"\n  [Spark SQL] Top-{K} by total_revenue:")
        spark.sql(f"""
            SELECT * FROM metrics_2024
            WHERE trips >= {int(min_support)}
            ORDER BY total_revenue DESC
            LIMIT {K}
        """).show(K, truncate=False)

        print(f"\n  [Spark SQL] Top-{K} by revenue_per_mile:")
        spark.sql(f"""
            SELECT * FROM metrics_2024
            WHERE trips >= {int(min_support)}
            ORDER BY revenue_per_mile DESC
            LIMIT {K}
        """).show(K, truncate=False)

        print(f"\n  [Spark SQL] Top-{K} by revenue_per_minute:")
        spark.sql(f"""
            SELECT * FROM metrics_2024
            WHERE trips >= {int(min_support)}
            ORDER BY revenue_per_minute DESC
            LIMIT {K}
        """).show(K, truncate=False)

        return res

    _, t_sql = timed("Spark SQL Parquet", sql_metrics)

    section("3. Ablation — DataFrame Parquet without Partition Pruning")

    def run_ablation():
        df_no_prune = enrich(spark.read.parquet(args.parquet2024))
        df_no_prune = apply_filter_no_pruning(df_no_prune)
        df_no_prune = add_distance_bucket(df_no_prune)
        df_no_prune = join_zones(df_no_prune, zone_df)
        m = compute_metrics(df_no_prune)
        m.cache()
        top_k_by(m, min_support, "total_revenue",     "total_revenue (ablation)")
        top_k_by(m, min_support, "revenue_per_mile",  "revenue_per_mile (ablation)")
        top_k_by(m, min_support, "revenue_per_minute","revenue_per_minute (ablation)")
        return m

    _, t_ablation = timed("DataFrame Parquet without pruning", run_ablation)

    print(f"\n  Parquet with pruning: {t_parq}s  |  Parquet without pruning: {t_ablation}s")
    print(f"  Partition-filter removal overhead: {round(t_ablation - t_parq, 2)}s")

    section("EXPLAIN — Partition Pruning")

    print("\n  [EXPLAIN with pruning]")
    spark.read.parquet(args.parquet2024).filter(
        F.col("pickup_day").isin(DAYS)
    ).explain(mode="formatted")

    print("\n  [EXPLAIN without pruning]")
    spark.read.parquet(args.parquet2024).explain(mode="formatted")

    section("Saving Results")

    out_metrics = f"{args.output}/q3_metrics_2024"
    (
        metrics_parq
        .filter(F.col("trips") >= min_support)
        .orderBy(F.desc("total_revenue"))
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(out_metrics)
    )
    print(f"  Saved: {out_metrics}")

    metrics_info = {
        "personalization": {
            "AM": AM, "H": H, "L": L,
            "hours": HOURS, "days": DAYS, "K": K,
        },
        "min_support": int(min_support),
        "total_trips_filtered": total_trips,
        "timings": {
            "csv_sec":      t_csv,
            "parquet_sec":  t_parq,
            "sql_sec":      t_sql,
            "ablation_sec": t_ablation,
        },
    }

    metrics_path = f"{args.output}/q3_metrics_info"

    hadoop_fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(metrics_path)
    if hadoop_fs.exists(hadoop_path):
        hadoop_fs.delete(hadoop_path, True)

    spark.sparkContext.parallelize(
        [json.dumps(metrics_info, indent=4, ensure_ascii=False)], 1
    ).saveAsTextFile(metrics_path)
    print(f"  Metrics: {metrics_path}")

    section("Q3 SUMMARY")
    print(f"  Personalization: hours {HOURS} | days {DAYS}")
    print(f"  Records after filtering: {total_trips:,}")
    print(f"  Min support: {int(min_support)}")
    print(f"  DataFrame CSV:                    {t_csv}s")
    print(f"  DataFrame Parquet (with pruning): {t_parq}s")
    print(f"  Spark SQL Parquet:                {t_sql}s")
    print(f"  Ablation (without pruning):       {t_ablation}s")

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
