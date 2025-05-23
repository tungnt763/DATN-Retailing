from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import csv
import tempfile
import urllib.request
import json
from datetime import datetime
import os

def fetch_weather(lat, lon, date, city, country):
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={date}&end_date={date}"
        "&hourly=temperature_2m,rain,showers,snowfall&timezone=UTC"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
            temps = data['hourly'].get('temperature_2m', [])
            rain = data['hourly'].get('rain', [])
            showers = data['hourly'].get('showers', [])
            snowfall = data['hourly'].get('snowfall', [])
            temp_avg = sum(temps)/len(temps) if temps else None
            rain_sum = sum(rain) if rain else None
            showers_sum = sum(showers) if showers else None
            snowfall_sum = sum(snowfall) if snowfall else None
            return {
                "weather_date": date,
                "city": city,
                "country": country,
                "latitude": lat,
                "longitude": lon,
                "temperature_avg": temp_avg,
                "precipitation": rain_sum,
                "rain": rain_sum,
                "showers": showers_sum,
                "snowfall": snowfall_sum,
            }
    except Exception as e:
        print(f"❌ Weather fetch error for {lat},{lon},{date}: {e}")
        return None


@task(provide_context=True)
def fetch_and_upload_weather_to_gcs(gcp_conn_id, project_name, dataset_name, bucket_name, table_name, **context):

    # Step 1️⃣: Query BigQuery để lấy (date, lat, lon, city, country)
    bq = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)

    sql = f"""
        SELECT DISTINCT
            FORMAT_DATE('%Y-%m-%d', DATE(trn_date)) AS weather_date,
            CAST(s.str_lat AS FLOAT64) AS latitude,
            CAST(s.str_lon AS FLOAT64) AS longitude,
            s.str_city AS city,
            s.str_cntry AS country
        FROM `{project_name}.{dataset_name}.transactions` t
        JOIN `{project_name}.{dataset_name}.stores` s
        ON t.trn_str_id = s.str_id
        WHERE s.str_lat IS NOT NULL AND s.str_lon IS NOT NULL
    """

    rows = bq.get_pandas_df(sql=sql, dialect="standard")

    print(f"✅ Retrieved {len(rows)} unique (date, lat, lon) combinations.")

    weather_records = []

    # Step 2️⃣: Fetch từ Open-Meteo API
    for _, row in rows.iterrows():
        print(f"Fetching weather data for {row.weather_date}, {row.city}, {row.country}")
        rec = fetch_weather(row.latitude, row.longitude, row.weather_date, row.city, row.country)
        if rec:
            weather_records.append(rec)

    print(f"✅ Total weather records fetched: {len(weather_records)}")

    # Step 3️⃣: Ghi file CSV tạm
    if not weather_records:
        print("⚠️ No weather data to write.")
        return

    fields = [
        "weather_date", "city", "country", "latitude", "longitude",
        "temperature_avg", "precipitation", "rain", "showers", "snowfall"
    ]

    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmpfile:
        writer = csv.DictWriter(tmpfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(weather_records)
        tmpfile_path = tmpfile.name

    # Step 4️⃣: Upload lên GCS
    gcs = GCSHook(gcp_conn_id=gcp_conn_id)
    object_path = f"raw/{table_name}/{table_name}_{context['ts_nodash']}.csv"
    gcs.upload(bucket_name=bucket_name, object_name=object_path, filename=tmpfile_path)

    print(f"✅ Uploaded weather file to gs://{bucket_name}/{object_path}")

    os.remove(tmpfile_path)
