import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, hour, dayofmonth, desc, unix_timestamp

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data2015", required=True)
    parser.add_argument("--data2024", required=True)
    parser.add_argument("--lookup", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Taxi_EDA_Final_Fixed").getOrCreate()


    df_2015 = spark.read.parquet(args.data2015)
    df_2024 = spark.read.parquet(args.data2024)
    df_lookup = spark.read.parquet(args.lookup)


    df_2015 = df_2015.withColumn("pickup_hour", hour("pickup_ts"))

    df_2024 = df_2024.withColumn("pickup_hour", hour("pickup_ts")) \
                     .withColumn("pickup_day", dayofmonth("pickup_ts")) \
                     .withColumn("duration_min", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("pickup_ts"))/60)

    results = {}


    results['describe_2015'] = [row.asDict() for row in df_2015.select("trip_distance", "total_amount").summary().collect()]
    results['describe_2024'] = [row.asDict() for row in df_2024.select("trip_distance", "total_amount").summary().collect()]


    def get_nulls(df):
        total = df.count()
        return df.select([(count(when(col(c).isNull(), c))/total*100).alias(c) for c in df.columns]).collect()[0].asDict()

    results['nulls_2015'] = get_nulls(df_2015)
    results['nulls_2024'] = get_nulls(df_2024)


    results['hour_2015'] = {str(r[0]): r[1] for r in df_2015.groupBy("pickup_hour").count().orderBy("pickup_hour").collect()}
    results['hour_2024'] = {str(r[0]): r[1] for r in df_2024.groupBy("pickup_hour").count().orderBy("pickup_hour").collect()}
    results['day_2024'] = {str(r[0]): r[1] for r in df_2024.groupBy("pickup_day").count().orderBy("pickup_day").collect()}


    def get_hist(df, column):
        rdd_data = df.select(column).filter((col(column) > 0) & (col(column) < 500)).rdd.flatMap(lambda x: x)
        bins, counts = rdd_data.histogram(50)
        return {"bins": list(bins), "counts": list(counts)}

    results['hist_dist_2024'] = get_hist(df_2024, "trip_distance")
    results['hist_amt_2024'] = get_hist(df_2024, "total_amount")


    if "PULocationID" in df_2024.columns:
        total_24 = df_2024.count()
        match_24 = df_2024.join(df_lookup, df_2024.PULocationID == df_lookup.location_id, "inner").count()
        results['joinability_2024_pct'] = (match_24 / total_24) * 100

        top_10 = df_2024.groupBy("PULocationID").count() \
            .join(df_lookup, col("PULocationID") == col("location_id")) \
            .select("zone", "count").orderBy(desc("count")).limit(10).collect()
        results['top_10_zones_2024'] = {r['zone']: r['count'] for r in top_10}


    results['anomalies_2024'] = {
        'max_dist': [r.asDict() for r in df_2024.orderBy(desc("trip_distance")).limit(5).select("trip_distance", "total_amount").collect()],
        'max_dur': [r.asDict() for r in df_2024.orderBy(desc("duration_min")).limit(5).select("duration_min", "trip_distance").collect()],
        'neg_fare': [r.asDict() for r in df_2024.filter(col("total_amount") < 0).limit(5).select("total_amount", "trip_distance").collect()]
    }


    final_json = json.dumps(results, indent=4)
    spark.sparkContext.parallelize([final_json], 1).saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    main()
