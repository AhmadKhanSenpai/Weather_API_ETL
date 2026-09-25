from datetime import datetime as dt
from datetime import timedelta as td

import requests
import pandas as pd
import time

URL = "https://api.open-meteo.com/v1/forecast"


# ----------------------------------------------------------------------
# 1) Build request params for ONE site
# ----------------------------------------------------------------------
def build_request_params(latitude, longitude):
    end_date = (dt.now() + td(days=1)).strftime("%Y-%m-%d")
    start_date = ((dt.now() + td(days=1)) - td(days=8)).strftime("%Y-%m-%d")

    return {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "shortwave_radiation"],
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
    }


# ----------------------------------------------------------------------
# 2) Call the API for ONE site.
#    Returns (response, outcome).
#    outcome is False or True.
# ----------------------------------------------------------------------
def call_weather_api(site_code, params):
    max_attempts = 5
    attempts = 0

    while True:
        try:
            response = requests.get(URL, params=params, timeout=30).json()

        except (requests.exceptions.RequestException, ValueError) as e:
            # RequestException = dropped connection, timeout, DNS failure, etc.
            # ValueError = response came back but wasn't valid JSON at all.
            attempts += 1
            print(f"[{site_code}] Network error: {e}")

            if attempts >= max_attempts:
                print(
                    f"[{site_code}] Failed after {max_attempts} attempts — moving on."
                )
                return None

            print(
                f"[{site_code}] Retrying in 15 seconds ({attempts}/{max_attempts})..."
            )
            time.sleep(15)
            continue

        if response.get("error"):
            reason = response.get("reason", "")
            print(f"[{site_code}] API error: {reason}")

            if "Hourly API request limit exceeded" in reason:
                print(f"[{site_code}] Hourly limit — waiting 1 hour.")
                time.sleep(3600)
                continue

            if "Minutely API request limit exceeded" in reason:
                print(f"[{site_code}] Minutely limit — waiting 1 minute.")
                time.sleep(60)
                continue

            if "Daily API request limit exceeded" in reason:
                print(f"[{site_code}] Daily limit — waiting 24 hours.")
                time.sleep(86400)
                continue

            attempts += 1
            if attempts >= max_attempts:
                print(
                    f"[{site_code}] Failed after {max_attempts} attempts — moving on."
                )
                return None

            print(
                f"[{site_code}] Retrying in 15 seconds ({attempts}/{max_attempts})..."
            )
            time.sleep(15)
            continue

        response["hourly"]["time"] = (
            pd.to_datetime(response["hourly"]["time"], utc=True)
            .strftime("%Y-%m-%d %H:%M:%S%z")
            .tolist()
        )

        response["hourly"]["site_code"] = site_code
        response["hourly"]["date"] = response["hourly"].pop("time")

        return response["hourly"]


# ----------------------------------------------------------------------
# 4) ONE unit of work: site in -> (DataFrame or None, outcome) out.
#    This is the exact function the Kafka consumer calls per message.
# ----------------------------------------------------------------------
def fetch_site_weather(site_code, latitude, longitude):
    params = build_request_params(latitude, longitude)
    response = call_weather_api(site_code, params)

    if response is None:
        return None

    return response
