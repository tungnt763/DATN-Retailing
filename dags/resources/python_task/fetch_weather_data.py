from pyspark.sql import SparkSession
from datetime import datetime
import urllib.request
import json
import argparse
import os
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def fetch_partition(partition):
    result = []
    with open("/tmp/spark_worker_log.txt", "a") as log:
        for row in partition:
            log.write(f"Fetching {row.weather_date} - {row.latitude},{row.longitude}\n")
            try:
                url = (
                    "https://archive-api.open-meteo.com/v1/archive?"
                    f"latitude={row.latitude}&longitude={row.longitude}&start_date={row.weather_date}&end_date={row.weather_date}"
                    "&hourly=temperature_2m,rain,showers,snowfall&timezone=UTC"
                )
                with urllib.request.urlopen(url, timeout=10) as response:
                    data = json.loads(response.read())
                    temps = data['hourly'].get('temperature_2m', [])
                    rain = data['hourly'].get('rain', [])
                    showers = data['hourly'].get('showers', [])
                    snowfall = data['hourly'].get('snowfall', [])
                    temp_avg = sum(temps)/len(temps) if temps else None
                    rain_sum = sum(rain) if rain else 0
                    showers_sum = sum(showers) if showers else 0
                    snowfall_sum = sum(snowfall) if snowfall else 0
                    precipitation = rain_sum + showers_sum + snowfall_sum
                    result.append((
                        row.weather_date,
                        row.city,
                        row.country,
                        row.latitude,
                        row.longitude,
                        temp_avg,
                        precipitation,
                        rain_sum,
                        showers_sum,
                        snowfall_sum,
                    ))
            except Exception as e:
                log.write(f"Failed fetch: {e}\n")
    return iter(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--dataset_name", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--bucket_name", required=True)
    parser.add_argument("--gcp_conn_id", required=True)
    args = parser.parse_args()

    # Spark Session
    spark = SparkSession.builder \
        .appName("weather-fetch") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/include/gcp/service_account.json") \
        .getOrCreate()

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    tmp_prefix = f"tmp/weather_{timestamp}/"
    final_path = f"raw/weather/weather_{timestamp}.csv"

    gcs_tmp_uri = f"gs://{args.bucket_name}/{tmp_prefix}"
    gcs_final_uri = f"gs://{args.bucket_name}/{final_path}"

    # Read from BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=args.gcp_conn_id, use_legacy_sql=False)
    sql = f"""
        SELECT DISTINCT
            FORMAT_DATE('%Y-%m-%d', DATE(trn_date)) AS weather_date,
            CAST(s.str_lat AS FLOAT64) AS latitude,
            CAST(s.str_lon AS FLOAT64) AS longitude,
            s.str_city AS city,
            s.str_cntry AS country
        FROM `{args.project_name}.{args.dataset_name}.transactions` t
        JOIN `{args.project_name}.{args.dataset_name}.stores` s
        ON t.trn_str_id = s.str_id
        WHERE s.str_lat IS NOT NULL AND s.str_lon IS NOT NULL
    """
    df = bq_hook.get_pandas_df(sql)
    print(f"✅ Retrieved {len(df)} records from BigQuery")

    if df.empty:
        print("⚠️ No data to process. Exiting.")
        spark.stop()
        exit(0)

    # Process with Spark
    df_spark = spark.createDataFrame(df)
    df_spark = df_spark.withColumn("latitude", df_spark["latitude"].cast("double"))
    df_spark = df_spark.withColumn("longitude", df_spark["longitude"].cast("double"))

    weather_rdd = df_spark.rdd.mapPartitions(fetch_partition)
    # print("⏳ Triggering RDD execution for debug")
    # print("RDD count:", weather_rdd.count())

    columns = [
        "weather_date", "city", "country", "latitude", "longitude",
        "temperature_avg", "precipitation", "rain", "showers", "snowfall"
    ]
    result_df = spark.createDataFrame(weather_rdd, columns)

    # Save as single part file
    result_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(gcs_tmp_uri)

    spark.stop()

    # Rename part file -> final csv using GCSHook
    gcs_hook = GCSHook(gcp_conn_id=args.gcp_conn_id)
    files = gcs_hook.list(bucket_name=args.bucket_name, prefix=tmp_prefix)

    part_file = next((f for f in files if f.endswith(".csv") and "part-" in f), None)
    if not part_file:
        raise Exception("No part CSV file found in GCS temp path")

    gcs_hook.copy(
        source_bucket=args.bucket_name,
        source_object=part_file,
        destination_bucket=args.bucket_name,
        destination_object=final_path
    )
    print(f"✅ Renamed {part_file} -> {final_path}")

    # Cleanup temp files
    for f in files:
        gcs_hook.delete(bucket_name=args.bucket_name, object_name=f)

    print(f"✅ Weather data written to: {gcs_final_uri}")
