import os

ELASTIC_PASSWORD = os.getenv("AIRFLOW_VAR_ELASTIC_PASSWORD")
AIRFLOW_VAR_FLIGHT_DATA_API_KEY = os.getenv("AIRFLOW_VAR_FLIGHT_DATA_API_KEY")
EMAIL_RECIPIENT = os.getenv("AIRFLOW_VAR_EMAIL_RECIPIENT")
INDEX = "flight_prices"
API_CONFIG = {
    "url": "https://sky-scanner3.p.rapidapi.com/flights/search-roundtrip",
    "params": {
        "fromEntityId": "eyJlIjoiMjc1NDc0NTQiLCJzIjoiV0FSUyIsImgiOiIyNzU0NzQ1NCIsInQiOiJDSVRZIn0="
    },
    "headers": {
        "x-rapidapi-key": AIRFLOW_VAR_FLIGHT_DATA_API_KEY,
        "x-rapidapi-host": "sky-scanner3.p.rapidapi.com",
    },
}
MIN_COUNT = 30
