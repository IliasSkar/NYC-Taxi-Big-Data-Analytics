import argparse
import json
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, LongType, TimestampType
)

AM = ""
H = ""
L     = 4
HOURS = ""
DAYS = ""

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


def parse_args():
    parser = argparse.ArgumentParser(description="Q4 Payment & Tip Analysis 2024")
    parser.add_argument("--parquet2024", required=True)
    parser.add_argument("--csv2024",     required=True)
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
    cols = df.columns

    if "tpep_pickup_datetime" in cols:
        df = df.withColumn("pickup_ts",
            F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    elif "pickup_ts" in cols:
        df = df.withColumn("pickup_ts", F.col("pickup_ts").cast(TimestampType()))

    if "tpep_dropoff_datetime" in cols:
        df = df.withColumn("dropoff_ts",
            F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    elif "dropoff_ts" in cols:
        df = df.withColumn("dropoff_ts", F.col("dropoff_ts").cast(TimestampType()))

    if "VendorID"     in df.columns: df = df.withColumnRenamed("VendorID",     "vendor_id")
    if "PULocationID" in df.columns: df = df.withColumnRenamed("PULocationID", "pu_location_id")
    if "DOLocationID" in df.columns: df = df.withColumnRenamed("DOLocationID", "do_location_id")
    if "Airport_fee"  in df.columns: df = df.withColumnRenamed("Airport_fee",  "airport_fee")

    if "pickup_hour" not in df.columns:
        df = df.withColumn("pickup_hour", F.hour("pickup_ts"))
    if "pickup_day" not in df.columns:
        df = df.withColumn("pickup_day",  F.dayofmonth("pickup_ts"))

    df = df.withColumn("duration_minutes",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0)

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


def apply_filter_csv(df):
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


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Q4_PaymentTipAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    section("Personalization Parameters")
    print(f"  AM={AM}  H={H}  L={L}")
    print(f"  Hours : {HOURS}")
    print(f"  Days  : {DAYS}")

    df_parq = enrich(spark.read.parquet(args.parquet2024))
    df_parq = apply_filter(df_parq)
    df_parq.cache()

    cnt = df_parq.count()
    print(f"\n  Parquet 2024 records (after filtering): {cnt:,}")

    df_parq.createOrReplaceTempView("trips_q4")

    section("1. Spark SQL — Hourly Payment Summary")

    def sql_hourly_payment():
        res = spark.sql("""
            SELECT
                pickup_hour,
                payment_type,
                COUNT(*) AS trips,
                ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY pickup_hour), 4)
                    AS payment_share_in_hour,
                ROUND(AVG(fare_amount), 2)      AS avg_fare_amount,
                ROUND(AVG(total_amount), 2)     AS avg_total_amount,
                ROUND(AVG(tip_amount), 2)       AS avg_tip_amount,
                ROUND(AVG(CASE WHEN fare_amount > 0
                               THEN tip_amount / fare_amount END), 4)
                    AS tip_rate,
                ROUND(SUM(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) / COUNT(*), 4)
                    AS zero_tip_share,
                ROUND(AVG(trip_distance), 2)    AS avg_trip_distance,
                ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes
            FROM trips_q4
            GROUP BY pickup_hour, payment_type
            ORDER BY pickup_hour, payment_type
        """)
        print("\n  [Spark SQL] Hourly Payment Summary:")
        res.show(50, truncate=False)
        return res

    hourly_pay_res, t_sql_hourly = timed("Spark SQL Hourly Payment", sql_hourly_payment)

    section("2. Spark SQL — Vendor vs Payment Summary")

    def sql_vendor_payment():
        res = spark.sql("""
            SELECT
                vendor_id,
                payment_type,
                COUNT(*) AS trips,
                ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY vendor_id), 4)
                    AS payment_share_in_vendor,
                ROUND(AVG(fare_amount), 2)  AS avg_fare_amount,
                ROUND(AVG(total_amount), 2) AS avg_total_amount,
                ROUND(AVG(tip_amount), 2)   AS avg_tip_amount,
                ROUND(AVG(CASE WHEN fare_amount > 0
                               THEN tip_amount / fare_amount END), 4)
                    AS avg_tip_rate,
                ROUND(SUM(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) / COUNT(*), 4)
                    AS zero_tip_share
            FROM trips_q4
            GROUP BY vendor_id, payment_type
            ORDER BY vendor_id, payment_type
        """)
        print("\n  [Spark SQL] Vendor x Payment Summary:")
        res.show(20, truncate=False)
        return res

    vendor_pay_res, t_sql_vendor = timed("Spark SQL Vendor Payment", sql_vendor_payment)

    section("3. Spark SQL — Card vs Cash by Hour")

    def sql_card_vs_cash():
        res = spark.sql("""
            SELECT
                pickup_hour,
                SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END)
                    AS card_trips,
                SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END)
                    AS cash_trips,
                ROUND(
                    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*), 4
                )   AS card_share,
                ROUND(AVG(CASE WHEN payment_type = 1 THEN total_amount END), 2)
                    AS avg_total_card,
                ROUND(AVG(CASE WHEN payment_type = 2 THEN total_amount END), 2)
                    AS avg_total_cash,
                ROUND(AVG(CASE WHEN payment_type = 1 THEN fare_amount END), 2)
                    AS avg_fare_card,
                ROUND(AVG(CASE WHEN payment_type = 2 THEN fare_amount END), 2)
                    AS avg_fare_cash,
                ROUND(AVG(CASE WHEN payment_type = 1 AND fare_amount > 0
                               THEN tip_amount / fare_amount END), 4)
                    AS avg_tip_rate_card
            FROM trips_q4
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """)
        print("\n  [Spark SQL] Card vs Cash by Hour:")
        res.show(truncate=False)
        return res

    card_cash_sql, t_sql_card_cash = timed("Spark SQL Card vs Cash", sql_card_vs_cash)

    section("4. DataFrame API — Card vs Cash by Hour")

    def df_card_vs_cash():
        res = (
            df_parq
            .groupBy("pickup_hour")
            .agg(
                F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("card_trips"),
                F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips"),
                F.round(
                    F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)) / F.count("*"), 4
                ).alias("card_share"),
                F.round(
                    F.avg(F.when(F.col("payment_type") == 1, F.col("total_amount"))), 2
                ).alias("avg_total_card"),
                F.round(
                    F.avg(F.when(F.col("payment_type") == 2, F.col("total_amount"))), 2
                ).alias("avg_total_cash"),
                F.round(
                    F.avg(F.when(F.col("payment_type") == 1, F.col("fare_amount"))), 2
                ).alias("avg_fare_card"),
                F.round(
                    F.avg(F.when(F.col("payment_type") == 2, F.col("fare_amount"))), 2
                ).alias("avg_fare_cash"),
                F.round(
                    F.avg(F.when(
                        (F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                        F.col("tip_amount") / F.col("fare_amount")
                    )), 4
                ).alias("avg_tip_rate_card"),
            )
            .orderBy("pickup_hour")
        )
        print("\n  [DataFrame API] Card vs Cash by Hour:")
        res.show(truncate=False)
        return res

    card_cash_df, t_df_card_cash = timed("DataFrame API Card vs Cash", df_card_vs_cash)

    section("5. CSV vs Parquet Comparison — Spark SQL Hourly Payment")

    df_csv = enrich(
        spark.read.option("header", "true").schema(SCHEMA_2024).csv(args.csv2024)
    )
    df_csv = apply_filter_csv(df_csv)
    df_csv.createOrReplaceTempView("trips_q4_csv")

    def sql_hourly_payment_csv():
        res = spark.sql("""
            SELECT
                pickup_hour,
                payment_type,
                COUNT(*) AS trips,
                ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY pickup_hour), 4)
                    AS payment_share_in_hour,
                ROUND(AVG(fare_amount), 2)      AS avg_fare_amount,
                ROUND(AVG(total_amount), 2)     AS avg_total_amount,
                ROUND(AVG(tip_amount), 2)       AS avg_tip_amount,
                ROUND(AVG(CASE WHEN fare_amount > 0
                               THEN tip_amount / fare_amount END), 4)
                    AS tip_rate,
                ROUND(SUM(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) / COUNT(*), 4)
                    AS zero_tip_share,
                ROUND(AVG(trip_distance), 2)    AS avg_trip_distance,
                ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes
            FROM trips_q4_csv
            GROUP BY pickup_hour, payment_type
            ORDER BY pickup_hour, payment_type
        """)
        res.show(50, truncate=False)
        return res

    _, t_csv_hourly = timed("Spark SQL CSV Hourly Payment", sql_hourly_payment_csv)

    print(f"\n  Spark SQL Parquet: {t_sql_hourly}s  |  Spark SQL CSV: {t_csv_hourly}s")
    print(f"  Speedup Parquet vs CSV: {round(t_csv_hourly / t_sql_hourly, 2)}x")

    section("Saving Results")

    out_hourly = f"{args.output}/q4_hourly_payment"
    hourly_pay_res.coalesce(1).write.mode("overwrite").parquet(out_hourly)
    print(f"  Saved: {out_hourly}")

    out_vendor = f"{args.output}/q4_vendor_payment"
    vendor_pay_res.coalesce(1).write.mode("overwrite").parquet(out_vendor)
    print(f"  Saved: {out_vendor}")

    out_card_cash = f"{args.output}/q4_card_vs_cash"
    card_cash_sql.coalesce(1).write.mode("overwrite").parquet(out_card_cash)
    print(f"  Saved: {out_card_cash}")

    metrics = {
        "personalization": {
            "AM": AM, "H": H, "L": L,
            "hours": HOURS, "days": DAYS,
        },
        "count_filtered": cnt,
        "timings": {
            "sql_hourly_payment_parquet_sec": t_sql_hourly,
            "sql_vendor_payment_sec":         t_sql_vendor,
            "sql_card_vs_cash_sec":           t_sql_card_cash,
            "df_card_vs_cash_sec":            t_df_card_cash,
            "sql_hourly_payment_csv_sec":     t_csv_hourly,
            "speedup_parquet_vs_csv":         round(t_csv_hourly / t_sql_hourly, 2),
        },
    }

    metrics_path = f"{args.output}/q4_metrics"
    spark.sparkContext.parallelize(
        [json.dumps(metrics, indent=4, ensure_ascii=False)], 1
    ).saveAsTextFile(metrics_path)
    print(f"  Metrics: {metrics_path}")

    section("Q4 SUMMARY")
    print(f"  Personalization: hours {HOURS} | days {DAYS}")
    print(f"  Records after filtering: {cnt:,}")
    print(f"  Spark SQL Hourly Payment (Parquet): {t_sql_hourly}s")
    print(f"  Spark SQL Vendor Payment:           {t_sql_vendor}s")
    print(f"  Spark SQL Card vs Cash:             {t_sql_card_cash}s")
    print(f"  DataFrame Card vs Cash:             {t_df_card_cash}s")
    print(f"  Spark SQL Hourly Payment (CSV):     {t_csv_hourly}s")
    print(f"  Speedup Parquet vs CSV:             {round(t_csv_hourly / t_sql_hourly, 2)}x")

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
