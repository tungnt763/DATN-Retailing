from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import json
import os
import tempfile
import csv
from datetime import datetime

@task(provide_context=True)
def fetch_currency_rates(gcp_conn_id, bucket_name, table_name, base_currency="USD", **context):
    try:
        api_key = os.getenv("OPENEXCHANGE_API_KEY")
        if not api_key:
            raise ValueError("Missing environment variable: OPENEXCHANGE_API_KEY")

        api_url = f"https://openexchangerates.org/api/latest.json?app_id={api_key}&base={base_currency}"

        # === Load metadata ===
        metadata_file = os.path.join(os.getenv("AIRFLOW_HOME"), "dags", "config", "currency_metadata.json")
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        metadata_df = pd.DataFrame.from_dict(metadata, orient="index")
        metadata_df.index.name = "currency_code"
        metadata_df.reset_index(inplace=True)

        # === Fetch rates from API ===
        response = requests.get(api_url)
        response.raise_for_status()
        rates = response.json()["rates"]

        # === Build DataFrame ===
        df_rates = pd.DataFrame(rates.items(), columns=["currency_code", "rate_from_base"])
        df_rates["rate_to_usd"] = df_rates["rate_from_base"].apply(
            lambda x: round(1.0 / x, 6)
        )

        # === Merge with metadata ===
        df = pd.merge(df_rates, metadata_df, on="currency_code", how="left")
        df["base_currency"] = base_currency

        fields = [
            "currency_code", "name", "symbol", "rate_to_usd", "base_currency"
        ]

        rows = df[fields].to_dict(orient='records')
        formatted_rows = [
            {k: (f"{v:.6f}" if isinstance(v, float) else v) for k, v in row.items()}
            for row in rows
        ]

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as tmpfile:
            writer = csv.DictWriter(tmpfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(formatted_rows)
            tmpfile_path = tmpfile.name

        gcs = GCSHook(gcp_conn_id=gcp_conn_id)

        now = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        object_path = f"raw/{table_name}/{year}/{month}/{day}/{hour}/{table_name}_{context['ts_nodash']}.csv"
        gcs.upload(bucket_name=bucket_name, object_name=object_path, filename=tmpfile_path)
        print(f"✅ Uploaded {len(df)} currency rates to gs://{bucket_name}/{object_path}")
        os.remove(tmpfile_path)

    except Exception as e:
        print("❌ Error during fetch_currency_rates:", str(e))
        raise
