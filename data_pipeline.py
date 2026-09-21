from datetime import datetime as dt
from datetime import timedelta as td

import openmeteo_requests
import pandas as pd
import database as db
import time
import sys

openmeteo = openmeteo_requests.Client()

URL = "https://api.open-meteo.com/v1/forecast"
MAX_ATTEMPTS = 5


def create_meta_table():
    db.create_sites_table()


def create_weather_data():
    db.create_weather_table()


def create_tracker_table():
    db.create_tracking_table()


def insert_meta_data(df):
    db.insert_meta_data(df)


def insert_weather_data(df):
    db.insert_weather_data(df)


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
#    outcome is "success", "retryable", or "permanent" — this is what
#    lets the caller decide whether to requeue the site later or give
#    up on it for good.
# ----------------------------------------------------------------------
def call_weather_api(site_code, params):
    attempts = 0

    while True:
        try:
            responses = openmeteo.weather_api(url=URL, params=params)
            return responses[0], "success"

        except Exception as e:
            error_message = str(e)
            print("\nEXCEPTION TYPE:", type(e))
            print("EXCEPTION:", repr(e))

            if "Hourly API request limit exceeded" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None, "retryable"
                print(
                    f"Hourly limit exceeded. Waiting 1 hour... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(3600)
                continue

            elif "Minutely API request limit exceeded" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None, "retryable"
                print(
                    f"Minutely limit exceeded. Waiting 1 minute... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(60)
                continue

            elif "Daily API request limit exceeded" in error_message:
                # NOTE: this still kills the whole process, not just this site.
                # In the old single-script pipeline that was fine — one run, one exit.
                # In consumer.py this now runs as a long-lived service, so this
                # sys.exit() takes down ALL partitions this consumer owns, not
                # just the one site it was working on. Worth revisiting if you
                # want the consumer to log it and keep running instead of dying.
                sys.exit(
                    "Daily API request limit reached, give it a rest see ya tomorrow"
                )

            elif "429" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None, "retryable"
                print(f"HTTP 429. Waiting 1 hour... Attempt {attempts}/{MAX_ATTEMPTS}")
                time.sleep(3600)
                continue

            elif any(code in error_message for code in ("500", "502", "503", "504")):
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Server error. Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None, "retryable"
                print(
                    f"Server error. Waiting 1 minute... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(60)
                continue

            elif "400" in error_message:
                print(
                    f"HTTP 400 - Bad Request for site {site_code}. Check API parameters."
                )
                return None, "permanent"

            elif "404" in error_message:
                print(f"HTTP 404 - Resource not found for site {site_code}.")
                return None, "permanent"

            else:
                print(f"Unknown API error for site {site_code}. Site marked as failed.")
                return None, "permanent"


# ----------------------------------------------------------------------
# 3) Turn ONE raw response into a DataFrame
# ----------------------------------------------------------------------
def parse_response_to_dataframe(site_code, response):
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
        ),
        "site_code": site_code,
        "temperature_2m": hourly_temperature_2m,
        "relative_humidity_2m": hourly_relative_humidity_2m,
        "global_tilted_irradiance_instant": hourly_global_tilted_irradiance_instant,
    }

    return pd.DataFrame(data=hourly_data)


# ----------------------------------------------------------------------
# 4) ONE unit of work: site in -> (DataFrame or None, outcome) out.
#    This is the exact function the Kafka consumer calls per message.
# ----------------------------------------------------------------------
def fetch_site_weather(site_code, latitude, longitude):
    params = build_request_params(latitude, longitude)
    response, outcome = call_weather_api(site_code, params)

    if response is None:
        return None, outcome

    return parse_response_to_dataframe(site_code, response), "success"
