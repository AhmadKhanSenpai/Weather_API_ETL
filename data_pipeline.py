from datetime import datetime as dt
from datetime import timedelta as td

import openmeteo_requests
import pandas as pd
import time

openmeteo = openmeteo_requests.Client()

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
        "timezone": "auto",
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
            responses = openmeteo.weather_api(url=URL, params=params)
            return responses[0]

        except Exception as e:
            error_message = str(e)
            print(f"[{site_code}] {type(e).__name__}: {e}")

            if "Hourly API request limit exceeded" in error_message:
                print(f"[{site_code}] Hourly limit — waiting 1 hour.")
                time.sleep(3600)
                continue

            if "Minutely API request limit exceeded" in error_message:
                print(f"[{site_code}] Minutely limit — waiting 1 minute.")
                time.sleep(60)
                continue

            if "Daily API request limit exceeded" in error_message:
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
                f"[{site_code}] Retrying in 15 seconds "
                f"({attempts}/{max_attempts})..."
            )
            time.sleep(15)


# ----------------------------------------------------------------------
# 3) Turn ONE raw response into a DataFrame
# ----------------------------------------------------------------------
def parse_response(site_code, response):
    hourly = response.Hourly()

    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_global_tilted_irradiance_instant = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
        .astype(str)
        .tolist(),
        "site_code": site_code,
        "temperature_2m": hourly_temperature_2m.tolist(),
        "relative_humidity_2m": hourly_relative_humidity_2m.tolist(),
        "global_tilted_irradiance_instant": hourly_global_tilted_irradiance_instant.tolist(),
    }

    return hourly_data


# ----------------------------------------------------------------------
# 4) ONE unit of work: site in -> (DataFrame or None, outcome) out.
#    This is the exact function the Kafka consumer calls per message.
# ----------------------------------------------------------------------
def fetch_site_weather(site_code, latitude, longitude):
    params = build_request_params(latitude, longitude)
    response = call_weather_api(site_code, params)

    if response is None:
        return None

    return parse_response(site_code, response)
