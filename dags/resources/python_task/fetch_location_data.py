from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import translate
from geopy.geocoders import Nominatim
import pandas as pd
import time
import csv
import tempfile
import os
from typing import List, Dict
from datetime import datetime
# ----- Reverse Geocoding -----
geolocator = Nominatim(user_agent="airflow-datn-retailing")

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse(f"{lat},{lon}", timeout=10)
        address = location.raw.get('address', {})
        city = address.get('city') or address.get('town') or 'Unknown'
        country = address.get('country', 'Unknown')
    except Exception as e:
        print(f"[ERROR] Geocoding failed for ({lat}, {lon}): {e}")
        city, country = 'Unknown', 'Unknown'
    time.sleep(1)
    return city, country

def fetch_store_location_data(bq, project_name, dataset_name, table_name):
    sql_unknown = f"""
        SELECT DISTINCT str_lat, str_lon
        FROM `{project_name}.{dataset_name}.{table_name}`
        WHERE str_city = 'Unknown' AND str_cntry = 'Unknown'
    """
    df_unknown = bq.get_pandas_df(sql=sql_unknown)
    df_unknown[['raw_city', 'raw_cntry']] = df_unknown.apply(
        lambda row: pd.Series(reverse_geocode(row['str_lat'], row['str_lon'])),
        axis=1
    )

    sql_known = f"""
        SELECT DISTINCT str_city AS raw_city, str_cntry AS raw_cntry
        FROM `{project_name}.{dataset_name}.{table_name}`
        WHERE str_city != 'Unknown' AND str_cntry != 'Unknown'
    """
    df_known = bq.get_pandas_df(sql=sql_known)

    df = pd.concat([
        df_known[['raw_city', 'raw_cntry']],
        df_unknown[['raw_city', 'raw_cntry']]
    ])
    return df.drop_duplicates()

# ----- Translation -----
def chunk_list(lst: List[str], chunk_size: int):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def batch_translate_chunked(
    texts: List[str],
    target_lang: str,
    project_id: str,
    chunk_size: int = 100,
    delay: float = 0.3
) -> Dict[str, str]:
    client = translate.TranslationServiceClient()
    parent = f"projects/{project_id}"
    translations = {}

    for chunk in chunk_list(texts, chunk_size):
        try:
            response = client.translate_text(
                parent=parent,
                contents=chunk,
                mime_type="text/plain",
                target_language_code=target_lang,
            )
            for original, translated in zip(chunk, response.translations):
                translations[original] = translated.translated_text
        except Exception as e:
            print(f"[ERROR] Translate failed for {chunk[:3]}...: {e}")
            for original in chunk:
                translations[original] = "Unknown"

        time.sleep(delay)
    return translations

# ----- Airflow Task -----
@task(provide_context=True)
def fetch_location_data(gcp_conn_id, project_name, dataset_name, table_name, bucket_name, **context):
    bq = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)

    if table_name == 'customers':
        sql = f"""
            SELECT DISTINCT cstmr_city AS raw_city, cstmr_cntry AS raw_cntry
            FROM `{project_name}.{dataset_name}.{table_name}` c
            WHERE NOT EXISTS (
                SELECT 1 FROM `{project_name}.edw.dim_locations` l
                WHERE c.cstmr_city = l.lct_raw_city AND c.cstmr_cntry = l.lct_raw_cntry
            )
        """
        df = bq.get_pandas_df(sql=sql)

    elif table_name == 'stores':
        df = fetch_store_location_data(bq, project_name, dataset_name, table_name)

    else:
        raise ValueError(f"[ERROR] Unsupported table: {table_name}")

    # ----- Translate -----
    city_list = [c for c in df['raw_city'].dropna().unique() if c and c != 'Unknown']
    cntry_list = [c for c in df['raw_cntry'].dropna().unique() if c and c != 'Unknown']

    city_trans = batch_translate_chunked(city_list, 'en', project_name)
    cntry_trans = batch_translate_chunked(cntry_list, 'en', project_name)

    df['translated_city'] = df['raw_city'].map(city_trans).fillna('Unknown')
    df['translated_country'] = df['raw_cntry'].map(cntry_trans).fillna('Unknown')

    fields = [
        "raw_city", "raw_cntry", "translated_city", "translated_country"
    ]
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmpfile:
        writer = csv.DictWriter(tmpfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(df[fields].to_dict(orient="records"))
        tmpfile_path = tmpfile.name

    gcs = GCSHook(gcp_conn_id=gcp_conn_id)

    now = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    hour = now.strftime('%H')
    object_path = f"raw/locations/{year}/{month}/{day}/{hour}/locations_{context['ts_nodash']}.csv"
    gcs.upload(bucket_name=bucket_name, object_name=object_path, filename=tmpfile_path)
    print(f"✅ Uploaded weather file to gs://{bucket_name}/{object_path}")
    os.remove(tmpfile_path)
