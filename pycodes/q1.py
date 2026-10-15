import argparse
import json
import os
import time
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType
)

AM = ""
H = ""
L         = 4
D = ""
K = ""
HOURS = ""
DAYS_2024 = ""


def parse_args():
    parser = argparse.ArgumentParser(description="Q1 Taxi Demand Analysis")
    parser.add_argument("--parquet2024", required=True)
    parser.add_argument("--csv2024",     required=True)
    parser.add_argument("--output",      required=True)
    return parser.parse_args()


SCHEMA_2024 = StructType([
    StructField("VendorID",              IntegerType(),  True),
    StructField("tpep_pickup_datetime",  TimestampType(),True),
    StructField("tpep_dropoff_datetime", TimestampType(),True),
    StructField("passenger_count",       DoubleType(),   True),
    StructField("trip_distance",         DoubleType(),   True),
    StructField("RatecodeID",            DoubleType(),   True),
    StructField("store_and_fwd_flag",    StringType(),   True),
    StructField("PULocationID",          IntegerType(),  True),
    StructField("DOLocationID",          IntegerType(),  True),
    StructField("payment_type",          IntegerType(),  True),
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
    if "pickup_ts" not in df.columns:
        df = df.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast(TimestampType()))
    if "dropoff_ts" not in df.columns:
        df = df.withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    df = df.withColumn("pickup_hour",  F.hour("pickup_ts"))
    df = df.withColumn("pickup_month", F.month("pickup_ts"))
    df = df.withColumn("pickup_day",   F.dayofmonth("pickup_ts"))
    df = df.withColumn("pickup_date",  F.to_date("pickup_ts"))
    df = df.withColumn("day_of_week",  F.dayofweek("pickup_ts"))
    df = df.withColumn("weekday",      F.date_format("pickup_ts", "EEEE"))
    df = df.withColumn("is_weekend",   F.col("day_of_week").isin([1, 7]))
    df = df.withColumn("duration_minutes",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    )
    df = df.withColumn("time_band",
        F.when(F.col("pickup_hour").between(0,  5),  "Night")
         .when(F.col("pickup_hour").between(6,  11), "Morning")
         .when(F.col("pickup_hour").between(12, 16), "Afternoon")
         .when(F.col("pickup_hour").between(17, 21), "Evening")
         .otherwise("Late")
    )
    return df


def apply_filter_2024(df):
    return df.filter(
        F.col("pickup_ts").isNotNull() &
        F.col("dropoff_ts").isNotNull() &
        F.col("duration_minutes").isNotNull() &
        (F.col("duration_minutes") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("total_amount") > 0) &
        F.col("pickup_day").isin(DAYS_2024) &
        F.col("pickup_hour").isin(HOURS)
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Q1_TaxiDemand")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    metrics = {}

    section("Personalization Parameters")
    print(f"  AM={AM}  H={H}  L={L}  D={D}  K={K}")
    print(f"  Hours    : {HOURS}")
    print(f"  Days     : {DAYS_2024}")

    section("Loading & Personalizing Parquet 2024")

    df_2024_raw = enrich(spark.read.parquet(args.parquet2024))
    df_2024 = apply_filter_2024(df_2024_raw)
    df_2024.cache()

    cnt_2024 = df_2024.count()
    print(f"  2024 records (after filtering): {cnt_2024:,}")

    section("1. RDD API — Demand by (pickup_date, pickup_hour) from CSV 2024")

    def rdd_demand_csv():
        df_csv = spark.read.option("header", "true").schema(SCHEMA_2024).csv(args.csv2024)
        df_csv = enrich(df_csv)

        rdd = df_csv.select(
            "pickup_date", "pickup_hour", "pickup_day", "total_amount",
            "duration_minutes", "trip_distance"
        ).rdd

        def valid(r):
            try:
                return (
                    r.pickup_date is not None and
                    r.pickup_hour in HOURS and
                    r.pickup_day in DAYS_2024 and
                    r.duration_minutes is not None and r.duration_minutes > 0 and
                    r.trip_distance is not None and r.trip_distance > 0 and
                    r.total_amount is not None and r.total_amount > 0
                )
            except:
                return False

        filtered = rdd.filter(valid)

        mapped = filtered.map(lambda r: (
            (str(r.pickup_date), r.pickup_hour),
            (1, r.total_amount)
        ))

        reduced = mapped.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
        results = reduced.map(lambda x: (x[0][0], x[0][1], x[1][0], round(x[1][1], 2)))
        top = results.sortBy(lambda x: (-x[2], -x[3])).take(K)

        print(f"\n  [RDD 2024] Top-{K} (pickup_date, pickup_hour, trips, total_revenue):")
        print(f"  {'Date':<14} {'Hour':>4} {'Trips':>12} {'Revenue':>14}")
        for row in top:
            print(f"  {row[0]:<14} {row[1]:>4} {row[2]:>12,} {row[3]:>14,.2f}")

        peak = top[0]
        return {"peak_date": peak[0], "peak_hour": peak[1],
                "peak_trips": peak[2], "peak_revenue": peak[3]}

    rdd_res_2024, t_rdd_2024 = timed("RDD 2024 CSV", rdd_demand_csv)

    metrics["rdd"] = {
        "2024": {**rdd_res_2024, "elapsed_sec": t_rdd_2024},
    }

    section("2. DataFrame API — Complete Q1 Metrics")

    def df_demand_full(df, label):
        total_trips = df.count()

        agg_expr = [
            F.count("*").alias("trips"),
            F.countDistinct("PULocationID").alias("unique_pickup_zones"),
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("total_amount").alias("total_revenue"),
        ]

        by_hour_date = (
            df.groupBy("pickup_date", "pickup_hour")
            .agg(*agg_expr)
            .withColumn("trip_share_in_personal_window",
                        F.round(F.col("trips") / total_trips * 100, 4))
            .orderBy(
                F.desc("trips"),
                F.desc("total_revenue"),
                F.asc("pickup_date"),
                F.asc("pickup_hour")
            )
        )

        print(f"\n  [{label}] Top-{K} hour-day combinations:")
        by_hour_date.show(K, truncate=False)

        by_timeband = (
            df.groupBy("time_band")
            .agg(
                F.count("*").alias("trips"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("trip_distance").alias("avg_distance"),
                F.avg("duration_minutes").alias("avg_duration")
            )
            .orderBy(F.desc("trips"))
        )
        print(f"  [{label}] Summary by time_band:")
        by_timeband.show(truncate=False)

        by_day = (
            df.groupBy("day_of_week", "weekday")
            .agg(F.count("*").alias("trips"))
            .orderBy(F.desc("trips"))
        )
        print(f"  [{label}] By Day of Week:")
        by_day.show(7, truncate=False)

        peak_row = by_hour_date.first()
        return {
            "peak_date": str(peak_row["pickup_date"]),
            "peak_hour": int(peak_row["pickup_hour"]),
            "peak_trips": int(peak_row["trips"]),
            "peak_revenue": float(peak_row["total_revenue"]),
        }

    df_csv_2024 = apply_filter_2024(enrich(
        spark.read.option("header", "true").schema(SCHEMA_2024).csv(args.csv2024)
    ))
    df_res_csv_2024, t_df_csv_2024 = timed("DataFrame CSV 2024",
        lambda: df_demand_full(df_csv_2024, "DataFrame CSV 2024"))

    df_res_parq_2024, t_df_parq_2024 = timed("DataFrame Parquet 2024",
        lambda: df_demand_full(df_2024, "DataFrame Parquet 2024"))

    metrics["dataframe"] = {
        "csv_2024":     {**df_res_csv_2024,  "elapsed_sec": t_df_csv_2024},
        "parquet_2024": {**df_res_parq_2024, "elapsed_sec": t_df_parq_2024},
    }

    section("3. Spark SQL — Parquet 2024")

    df_2024.createOrReplaceTempView("trips_2024")

    def sql_demand():
        total_trips = spark.sql("SELECT COUNT(*) as c FROM trips_2024").first()["c"]

        res = spark.sql(f"""
            SELECT
                pickup_date,
                pickup_hour,
                COUNT(*) AS trips,
                COUNT(DISTINCT PULocationID) AS unique_pickup_zones,
                ROUND(AVG(passenger_count), 2)    AS avg_passenger_count,
                ROUND(AVG(duration_minutes), 2)   AS avg_duration_minutes,
                ROUND(AVG(trip_distance), 2)       AS avg_trip_distance,
                ROUND(AVG(total_amount), 2)        AS avg_total_amount,
                ROUND(SUM(total_amount), 2)        AS total_revenue,
                ROUND(COUNT(*) / {total_trips} * 100, 4) AS trip_share_in_personal_window
            FROM trips_2024
            GROUP BY pickup_date, pickup_hour
            ORDER BY trips DESC, total_revenue DESC, pickup_date ASC, pickup_hour ASC
            LIMIT {K}
        """)
        print(f"\n  [SQL 2024] Top-{K}:")
        res.show(K, truncate=False)

        spark.sql("""
            SELECT time_band,
                   COUNT(*) AS trips,
                   ROUND(SUM(total_amount), 2) AS total_revenue,
                   ROUND(AVG(trip_distance), 2) AS avg_distance,
                   ROUND(AVG(duration_minutes), 2) AS avg_duration
            FROM trips_2024
            GROUP BY time_band
            ORDER BY trips DESC
        """).show(truncate=False)

        peak_row = res.first()
        return {
            "peak_date":    str(peak_row["pickup_date"]),
            "peak_hour":    int(peak_row["pickup_hour"]),
            "peak_trips":   int(peak_row["trips"]),
            "peak_revenue": float(peak_row["total_revenue"]),
        }

    sql_res_2024, t_sql_2024 = timed("SQL 2024", sql_demand)

    metrics["sql"] = {
        "2024": {**sql_res_2024, "elapsed_sec": t_sql_2024},
    }

    section("4. CSV vs Parquet — Runtime Comparison 2024")

    def query_parquet():
        return (
            df_2024.groupBy("pickup_date", "pickup_hour")
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
            .orderBy(F.desc("trips"))
            .collect()
        )

    def query_csv():
        df = apply_filter_2024(enrich(
            spark.read.option("header", "true").schema(SCHEMA_2024).csv(args.csv2024)
        ))
        return (
            df.groupBy("pickup_date", "pickup_hour")
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
            .orderBy(F.desc("trips"))
            .collect()
        )

    _, t_parq_2024 = timed("Parquet 2024", query_parquet)
    _, t_csv_2024  = timed("CSV 2024",     query_csv)

    sp_2024 = round(t_csv_2024 / t_parq_2024, 2) if t_parq_2024 > 0 else 0

    print(f"  2024 → CSV: {t_csv_2024}s  Parquet: {t_parq_2024}s  Speedup: {sp_2024}x")
    print(f"  Parquet: columnar storage + partition pruning → reads only the required columns.")
    print(f"  RDD: more limited because it does not leverage the Catalyst optimizer and the Tungsten engine.")

    metrics["csv_vs_parquet"] = {
        "2024": {"csv_sec": t_csv_2024, "parquet_sec": t_parq_2024, "speedup_x": sp_2024},
    }

    section("Saving Results")

    out_path = f"{args.output}/hourly_demand_2024"
    (
        df_2024.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_amount").alias("total_revenue")
        )
        .orderBy("pickup_date", "pickup_hour")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(out_path)
    )
    print(f"  Saved: {out_path}")

    metrics["personalization"] = {
        "AM": AM, "H": H, "L": L, "D": D, "K": K,
        "hours": HOURS, "days_2024": DAYS_2024,
        "count_2024_filtered": cnt_2024,
    }

    metrics_path = f"{args.output}/q1_metrics"
    spark.sparkContext.parallelize(
        [json.dumps(metrics, indent=4, ensure_ascii=False)], 1
    ).saveAsTextFile(metrics_path)
    print(f"  Metrics: {metrics_path}")

    section("Q1 SUMMARY")
    print(f"  Personalization: hours {HOURS} | days 2024: {DAYS_2024} | K={K}")
    print(f"  2024 ({cnt_2024:,} records): RDD peak={rdd_res_2024['peak_date']} {rdd_res_2024['peak_hour']:02d}:00")
    print(f"  DataFrame Parquet 2024 peak: {df_res_parq_2024['peak_date']} {df_res_parq_2024['peak_hour']:02d}:00  trips={df_res_parq_2024['peak_trips']:,}")
    print(f"  SQL 2024 peak: {sql_res_2024['peak_date']} {sql_res_2024['peak_hour']:02d}:00  trips={sql_res_2024['peak_trips']:,}")
    print(f"  Speedup Parquet 2024: {sp_2024}x")

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
