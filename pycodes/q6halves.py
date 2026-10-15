import argparse
import json
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

K = ""
MIN_TRIPS = 10


def parse_args():
    parser = argparse.ArgumentParser(
        description="Q6 OD-Halves — Origin-Destination scaling experiment (full 2024)"
    )
    parser.add_argument("--parquet2024", required=True,
                        help="Input location for yellow_tripdata_2024")
    parser.add_argument("--output",      required=True,
                        help="Output location")
    parser.add_argument("--run-label",   required=True,
                        help="Run label, e.g. 'A_cold', 'A_warm', 'B_cold', etc.")
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
        .appName(f"Q6_OD_Halves_{args.run_label}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    section(f"Q6 OD-Halves | run={args.run_label}")
    print(f"  Dataset        : full 2024 dataset (without personalization)")
    print(f"  K              : {K}")
    print(f"  Min trips/half : >= {MIN_TRIPS}")
    print(f"  executor.instances : {sc.getConf().get('spark.executor.instances', 'N/A')}")
    print(f"  executor.cores     : {sc.getConf().get('spark.executor.cores',     'N/A')}")
    print(f"  executor.memory    : {sc.getConf().get('spark.executor.memory',    'N/A')}")

    raw = spark.read.parquet(args.parquet2024)

    df = (
        raw
        .withColumnRenamed("VendorID",     "vendor_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumn(
            "dropoff_ts",
            F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        )
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
        )
        .filter(
            (F.year("pickup_ts") == 2024)
            & F.col("pickup_ts").isNotNull()
            & F.col("dropoff_ts").isNotNull()
            & (F.col("duration_minutes") > 0)
            & (F.col("trip_distance")    > 0)
            & (F.col("fare_amount")      > 0)
        )
    )

    section("OD-Halves Analysis")

    def od_halves():

        h1 = (
            df.filter(F.col("pickup_day") <= 15)
            .groupBy("pu_location_id", "do_location_id", "pickup_hour")
            .agg(
                F.count("*").alias("trips_h1"),
                F.round(F.avg("fare_amount"),  2).alias("avg_fare_h1"),
                F.round(F.avg("total_amount"), 2).alias("avg_total_h1"),
            )
        )

        h2 = (
            df.filter(F.col("pickup_day") > 15)
            .groupBy("pu_location_id", "do_location_id", "pickup_hour")
            .agg(
                F.count("*").alias("trips_h2"),
                F.round(F.avg("fare_amount"),  2).alias("avg_fare_h2"),
                F.round(F.avg("total_amount"), 2).alias("avg_total_h2"),
            )
        )

        od = (
            h1.join(h2,
                on=["pu_location_id", "do_location_id", "pickup_hour"],
                how="inner"
            )
            .filter(
                (F.col("trips_h1") >= MIN_TRIPS)
                & (F.col("trips_h2") >= MIN_TRIPS)
            )
            .withColumn("trips_change_pct",
                F.round(
                    (F.col("trips_h2") - F.col("trips_h1")) * 100.0 / F.col("trips_h1"),
                    2
                )
            )
            .withColumn("fare_change_pct",
                F.round(
                    (F.col("avg_fare_h2") - F.col("avg_fare_h1")) * 100.0 / F.col("avg_fare_h1"),
                    2
                )
            )
        )

        od.cache()
        total_od = od.count()
        print(f"\n  OD pairs after filtering (trips >= {MIN_TRIPS}): {total_od:,}")

        print("\n  [OD-Halves] Explain (formatted):")
        od.explain(mode="formatted")

        top_increase = od.orderBy(F.desc("trips_change_pct")).limit(K)
        print(f"\n  [OD-Halves] Top-{K} pairs with the largest INCREASE (trips_change_pct):")
        top_increase.select(
            "pu_location_id", "do_location_id", "pickup_hour",
            "trips_h1", "trips_h2", "trips_change_pct",
            "avg_fare_h1", "avg_fare_h2", "fare_change_pct"
        ).show(K, truncate=False)

        top_decrease = od.orderBy(F.asc("trips_change_pct")).limit(K)
        print(f"\n  [OD-Halves] Top-{K} pairs with the largest DECREASE (trips_change_pct):")
        top_decrease.select(
            "pu_location_id", "do_location_id", "pickup_hour",
            "trips_h1", "trips_h2", "trips_change_pct",
            "avg_fare_h1", "avg_fare_h2", "fare_change_pct"
        ).show(K, truncate=False)

        return od, top_increase, top_decrease

    (od, top_increase, top_decrease), t_od = timed("OD-Halves Full Analysis", od_halves)

    section("Saving Results")

    out_base = f"{args.output}/q6_od_halves/{args.run_label}"

    top_increase.coalesce(1).write.mode("overwrite").parquet(f"{out_base}/top_increase")
    top_decrease.coalesce(1).write.mode("overwrite").parquet(f"{out_base}/top_decrease")
    print(f"  Saved top_increase : {out_base}/top_increase")
    print(f"  Saved top_decrease : {out_base}/top_decrease")

    metrics = {
        "run_label": args.run_label,
        "executor_config": {
            "instances": sc.getConf().get("spark.executor.instances", "N/A"),
            "cores":     sc.getConf().get("spark.executor.cores",     "N/A"),
            "memory":    sc.getConf().get("spark.executor.memory",    "N/A"),
        },
        "params": {
            "K": K,
            "min_trips_per_half": MIN_TRIPS,
        },
        "timings": {
            "od_halves_sec": t_od,
        },
    }

    spark.createDataFrame(
        [(json.dumps(metrics, indent=4, ensure_ascii=False),)], ["value"]
    ).coalesce(1).write.mode("overwrite").text(f"{out_base}/metrics")
    print(f"  Metrics: {out_base}/metrics")

    section(f"OD-Halves SUMMARY | run={args.run_label}")
    print(f"  Executor instances : {sc.getConf().get('spark.executor.instances', 'N/A')}")
    print(f"  Executor cores     : {sc.getConf().get('spark.executor.cores',     'N/A')}")
    print(f"  Executor memory    : {sc.getConf().get('spark.executor.memory',    'N/A')}")
    print(f"  OD-Halves          : {t_od}s")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
