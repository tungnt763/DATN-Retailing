from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import csv
import tempfile
import urllib.request
import json
import time
from datetime import datetime
import os

def fetch_weather(lat, lon, date, city, country):
    API_KEY = os.getenv("VISUAL_CROSSING_API_KEY")  # Cần đặt biến môi trường trong Airflow hoặc .env
    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{lat},{lon}/{date}?unitGroup=metric&key={API_KEY}&contentType=json"
    )
    print(url)
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
            day_data = data.get('days', [{}])[0]

            return {
                "weather_date": date,
                "city": city,
                "country": country,
                "latitude": lat,
                "longitude": lon,
                "temperature_avg": day_data.get("temp"),        # Nhiệt độ trung bình (°C)
                "precipitation": day_data.get("precip", 0),        # Tổng lượng mưa (mm)
                "rain": day_data.get("precipcover", 0),            
                "showers": day_data.get("precipprob", 0),       # Xác suất mưa (%)
                "snowfall": day_data.get("snow", 0),               # Lượng tuyết rơi (mm)
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
