import sys
import os
from datetime import datetime
import requests
import numpy as np
import pandas as pd
from airflow.exceptions import AirflowException

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from elasticsearch_utils import ElasticsearchConnection
from config import API_CONFIG


URL = API_CONFIG["url"]
PARAMS = API_CONFIG["params"]
HEADERS = API_CONFIG["headers"]


def fetch_data(**kwargs) -> str:
    """Fetches current flight price data"""
    response = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=20)
    if response.status_code != 200:
        raise AirflowException(
            f"Error fecthing data. Status code {response.status_code}"
        )
    data = response.json()["data"]["everywhereDestination"]["results"]
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    for entry in data:
        entry["timestamp"] = current_time
    kwargs["ti"].xcom_push(key="fetched_data", value=data)


def prepare_price_alerts(location_price_statistics, **kwargs):
    """Prepares flight price data for indexing and prepares subset for email notification"""
    data = kwargs["ti"].xcom_pull(task_ids="fetch_data", key="fetched_data")
    flight_prices_df = pd.json_normalize(data, sep="_")
    if flight_prices_df.empty:
        raise AirflowException("No data found.")
    flight_prices_df["cheapest_price"] = np.where(
        flight_prices_df["content_flightQuotes_cheapest_direct"],
        flight_prices_df["content_flightQuotes_cheapest_rawPrice"],
        flight_prices_df["content_flightQuotes_direct_rawPrice"],
    )
    flight_prices_df = flight_prices_df[flight_prices_df["cheapest_price"] >= 0]
    flight_prices_df["content_location_name"] = flight_prices_df[
        "content_location_name"
    ].replace(r" ", "_", regex=True)
    flight_prices_df = (
        flight_prices_df.loc[
            :, ["skyId", "content_location_name", "cheapest_price", "timestamp"]
        ]
        .rename(columns={"content_location_name": "location", "skyId": "sky_id"})
        .dropna()
    )
    kwargs["ti"].xcom_push(
        key="all_rows", value=flight_prices_df.to_dict(orient="records")
    )
    if location_price_statistics:
        flight_prices_df[["average_price", "std_dev"]] = flight_prices_df[
            "location"
        ].apply(
            lambda loc: pd.Series(location_price_statistics.get(loc, (np.nan, np.nan)))
        )
        flight_prices_df = flight_prices_df.dropna(subset=["average_price", "std_dev"])
        low_flight_prices_df = flight_prices_df[
            flight_prices_df["cheapest_price"]
            < np.maximum(
                flight_prices_df["average_price"] - 0.5 * flight_prices_df["std_dev"],
                flight_prices_df["average_price"] * 0.9,
            )
        ].drop(columns=["std_dev"])
        kwargs["ti"].xcom_push(
            key="rows_to_notify", value=low_flight_prices_df.to_dict(orient="records")
        )


def check_for_price_alerts(**kwargs):
    prepare_price_alerts(
        ElasticsearchConnection.get_location_price_statistics(), **kwargs
    )


def should_send_email(**kwargs):
    rows_to_notify = kwargs["ti"].xcom_pull(
        key="rows_to_notify", task_ids="prepare_price_alerts"
    )
    return bool(rows_to_notify)
