from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import time

spark = SparkSession.builder.appName("NYC_Taxi_Final_Prep").getOrCreate()

USER = ""
BASE_INPUT = ""
BASE_OUTPUT = ""

schema_2015 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

schema_2024 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

def process_large_dataset(year, schema, partition_col):
    start_time = time.time()
    input_path = f"{BASE_INPUT}/yellow_tripdata_{year}.csv"
    output_path = f"{BASE_OUTPUT}/yellow_tripdata_{year}"

    df = spark.read.format("csv").option("header", "true").schema(schema).load(input_path)
    raw_count = df.count()
    null_ts = df.filter(F.col("tpep_pickup_datetime").isNull()).count()

    df_cleaned = df.withColumnRenamed("tpep_pickup_datetime", "pickup_ts") \
                   .filter(F.year(F.col("pickup_ts")) == int(year))
    
    if partition_col == "pickup_hour":
        df_cleaned = df_cleaned.withColumn("pickup_hour", F.hour(F.col("pickup_ts")))
    else:
        df_cleaned = df_cleaned.withColumn("pickup_day", F.dayofmonth(F.col("pickup_ts")))

    final_count = df_cleaned.count()
    range_ts = df_cleaned.select(F.min("pickup_ts"), F.max("pickup_ts")).collect()[0]

    df_cleaned.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)

    return {
        "dataset": f"yellow_{year}",
        "raw_rows": raw_count,
        "prepared_rows": final_count,
        "null_timestamps": null_ts,
        "rejected_rows": raw_count - final_count - null_ts,
        "min_ts": str(range_ts[0]),
        "max_ts": str(range_ts[1]),
        "duration_sec": round(time.time() - start_time, 2),
        "partition_col": partition_col
    }

metrics = []
metrics.append(process_large_dataset("2015", schema_2015, "pickup_hour"))
metrics.append(process_large_dataset("2024", schema_2024, "pickup_day"))

metrics_df = spark.createDataFrame(metrics)
metrics_df.coalesce(1).write.mode("overwrite").json("")

spark.stop()
