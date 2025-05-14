from airflow.decorators import task
import requests
import pandas as pd
import json
import os
from datetime import datetime

@task
def fetch_currency_rates(output_path: str):
    """
    Fetch daily currency rates from OpenExchangeRates and enrich with metadata.
    Output is saved as a CSV for use in BigQuery pipelines.
    """
    API_KEY = os.getenv("OPENEXCHANGE_API_KEY")
    BASE_CURRENCY = "USD"
    API_URL = f"https://openexchangerates.org/api/latest.json?app_id={API_KEY}&base={BASE_CURRENCY}"

    # Load static currency metadata (includes name and symbol)
    metadata_path = os.path.join(os.path.dirname("AIRFLOW_HOME"), "dags", "config", "currency_metadata.json")
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    metadata_df = pd.DataFrame.from_dict(metadata, orient="index")
    metadata_df.index.name = "currency_code"
    metadata_df.reset_index(inplace=True)

    try:
        # Fetch exchange rates
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Create DataFrame from rates
        rates_df = pd.DataFrame(data["rates"].items(), columns=["currency_code", "rate_from_base"])

        # Calculate rate_to_usd (if base is not USD)
        rates_df["rate_to_usd"] = rates_df["rate_from_base"].apply(
            lambda x: round(1.0 / x, 6) if BASE_CURRENCY != "USD" else round(x, 6)
        )

        # Merge with metadata
        merged_df = pd.merge(rates_df, metadata_df, on="currency_code", how="left")

        merged_df["base_currency"] = BASE_CURRENCY

        # Final columns for BigQuery
        final_df = merged_df[[
            "currency_code", "name", "symbol", "rate_to_usd", "base_currency"
        ]].rename(columns={
            "name": "currency_name",
            "symbol": "currency_symbol"
        })

        # Save as CSV
        final_df.to_csv(output_path, index=False, float_format="%.6f")
        print(f">> Saved {len(final_df)} rows to {output_path}")

    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch currency rates: {e}")
