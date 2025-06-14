import asyncio
import aiohttp
from aiohttp import ClientSession, TCPConnector
from asyncio import Semaphore
import pandas as pd
import csv
import tempfile
import os
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import nest_asyncio
from datetime import datetime
nest_asyncio.apply()

# ----------------------------
# Async Weather API Caller with Retry Logic
# ----------------------------
async def fetch_weather_async(session: ClientSession, semaphore: Semaphore,
                              lat: float, lon: float, date: str, city: str, country: str):
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={date}&end_date={date}"
        "&hourly=temperature_2m,rain,showers,snowfall&timezone=UTC"
    )

    async with semaphore:
        for attempt in range(5):
            try:
                async with session.get(url, timeout=10) as response:
                    data = await response.json()

                    if data.get("reason", "").lower().startswith("minutely api request limit"):
                        wait_time = 60 * (attempt + 1)
                        print(f"⚠️ Rate limited, retrying in {wait_time} seconds for {lat},{lon},{date}")
                        await asyncio.sleep(wait_time)
                        continue

                    if not isinstance(data, dict) or "hourly" not in data:
                        raise ValueError(f"Missing 'hourly' in API response: {data}")

                    hourly = data["hourly"]
                    temps = hourly.get('temperature_2m', [])
                    rain = hourly.get('rain', [])
                    showers = hourly.get('showers', [])
                    snowfall = hourly.get('snowfall', [])
                    temp_avg = round(sum(temps)/len(temps), 2) if temps else None
                    rain_sum = round(sum(rain), 2) if rain else None
                    showers_sum = round(sum(showers), 2) if showers else None
                    snowfall_sum = round(sum(snowfall), 2) if snowfall else None

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
                if attempt < 4:
                    wait_time = 2 ** attempt
                    print(f"❌ Error fetching ({lat},{lon},{date}): {e} - retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ Final failure for ({lat},{lon},{date}): {e}")
                    return None


# ----------------------------
# Batch controller
# ----------------------------
async def fetch_all_weather_in_batches(rows: pd.DataFrame, batch_size=100):
    results = []
    semaphore = Semaphore(10)
    connector = TCPConnector(limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(rows), batch_size):
            batch = rows.iloc[i:i+batch_size]
            tasks = [
                fetch_weather_async(session, semaphore,
                                    row.latitude, row.longitude,
                                    row.weather_date, row.city, row.country)
                for _, row in batch.iterrows()
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend([r for r in batch_results if r])
            print(f"✅ Completed batch {i // batch_size + 1}/{(len(rows) - 1) // batch_size + 1} — total fetched: {len(results)}")
            if i // batch_size + 1 == (len(rows) - 1) // batch_size + 1:
                break
            await asyncio.sleep(60)  # cooldown để tránh giới hạn mỗi phút

    return results


# ----------------------------
# Airflow task to fetch and upload
# ----------------------------
@task(provide_context=True)
def fetch_and_upload_weather_to_gcs(gcp_conn_id, project_name, dataset_name, bucket_name, table_name, **context):
    # Step 1️⃣: Lấy dữ liệu từ BigQuery
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
    rows = bq.get_pandas_df(sql=sql)
    print(f"✅ Retrieved {len(rows)} unique (date, lat, lon) combinations.")

    # Step 2️⃣: Gọi API thời tiết với batching + retry
    weather_records = asyncio.run(fetch_all_weather_in_batches(rows))
    print(f"✅ Total weather records fetched: {len(weather_records)}")

    if not weather_records:
        print("⚠️ No weather data to write.")
        return

    # Step 3️⃣: Ghi vào file CSV tạm
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

    now = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    hour = now.strftime('%H')
    object_path = f"raw/{table_name}/{year}/{month}/{day}/{hour}/{table_name}_{context['ts_nodash']}.csv"
    gcs.upload(bucket_name=bucket_name, object_name=object_path, filename=tmpfile_path)
    print(f"✅ Uploaded weather file to gs://{bucket_name}/{object_path}")
    os.remove(tmpfile_path)
