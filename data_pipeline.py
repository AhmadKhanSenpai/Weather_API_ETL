from datetime import datetime as dt
from datetime import timedelta as td

import openmeteo_requests
import pandas as pd
import database as db
import time
import sys

openmeteo = openmeteo_requests.Client()

URL = "https://api.open-meteo.com/v1/forecast"
PATH = "meta_data.csv"
MAX_ATTEMPTS = 5


def create_meta_table():
    db.create_sites_table()


def create_weather_data():
    db.create_weather_table()


def create_tracker_table():
    db.create_tracking_table()


def insert_meta_data(path):
    df = pd.read_csv(path)
    db.insert_meta_data(df)


def insert_weather_data(df):
    db.insert_weather_data(df)


# Build request params for ONE site
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


# Call the API for ONE site — all retry/backoff logic lives here only
def call_weather_api(site_code, params):
    """Returns the raw Open-Meteo response object for this site, or None
    if it could not be fetched after retries."""
    attempts = 0

    while True:

        try:
            responses = openmeteo.weather_api(url=URL, params=params)
            return responses[0]

        except Exception as e:
            error_message = str(e)
            print("\nEXCEPTION TYPE:", type(e))
            print("EXCEPTION:", repr(e))

            # ----------------------------------
            # Open-Meteo hourly request limit
            # ----------------------------------
            if "Hourly API request limit exceeded" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None
                print(
                    f"Hourly limit exceeded. Waiting 1 hour... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(3600)
                continue

            # ----------------------------------
            # Open-Meteo minutely request limit
            # ----------------------------------
            elif "Minutely API request limit exceeded" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None
                print(
                    f"Minutely limit exceeded. Waiting 1 minute... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(60)
                continue

            # ----------------------------------
            # Open-Meteo daily request limit
            # ----------------------------------
            elif "Daily API request limit exceeded" in error_message:

                sys.exit(
                    "Daily API request limit reached, give it a rest see ya tomorrow"
                )

            elif "429" in error_message:
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None
                print(f"HTTP 429. Waiting 1 hour... Attempt {attempts}/{MAX_ATTEMPTS}")
                time.sleep(3600)
                continue

            elif any(code in error_message for code in ("500", "502", "503", "504")):
                attempts += 1
                if attempts >= MAX_ATTEMPTS:
                    print(
                        f"Server error. Maximum attempts ({MAX_ATTEMPTS}) reached for site {site_code}."
                    )
                    return None
                print(
                    f"Server error. Waiting 1 minute... Attempt {attempts}/{MAX_ATTEMPTS}"
                )
                time.sleep(60)
                continue

            elif "400" in error_message:
                print(
                    f"HTTP 400 - Bad Request for site {site_code}. Check API parameters."
                )
                return None

            elif "404" in error_message:
                print(f"HTTP 404 - Resource not found for site {site_code}.")
                return None

            else:
                print(f"Unknown API error for site {site_code}. Site marked as failed.")
                return None


# Turn ONE raw response into a DataFrame
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


# ONE unit of work: site in -> DataFrame or None out.
# This is the function that will become your Kafka consumer's
# per-message handler later — that's why it's isolated like this.
def fetch_site_weather(site_code, latitude, longitude):
    params = build_request_params(latitude, longitude)
    response = call_weather_api(site_code, params)

    if response is None:
        return None

    return parse_response_to_dataframe(site_code, response)


# Each site is fetched, inserted, and tracked as a single unit.
def process_sites(df):
    for row in df.itertuples(index=False):
        result_df = fetch_site_weather(row.site_code, row.latitude, row.longitude)

        if result_df is not None:
            insert_weather_data(result_df)
            db.update_tracking_status([row.site_code], status=True)
        else:
            db.update_tracking_status([row.site_code], status=False)


def new_sites_fetch_data(path):
    insert_meta_data(path)

    df = pd.read_csv(path)
    tracker = db.read_parsed_sites()

    # in case if the API is interupted due to any reason
    mask = ~df["site_code"].isin(tracker["site_code"])
    df = df.loc[mask]

    if df.empty:
        return

    process_sites(df)


def failed_sites_retry(path):
    max_attempts = 10
    attempts = 0
    df = pd.read_csv(path)  # meta data doesn't change between retries — read once

    while True:
        failed_sites = db.read_failed_sites()

        if failed_sites.empty:
            break
        if attempts >= max_attempts:
            break

        mask = df["site_code"].isin(failed_sites["site_code"])
        df_failed_sites = df.loc[mask]

        before = len(df_failed_sites)
        process_sites(df_failed_sites)

        still_failed = db.read_failed_sites()
        if len(still_failed) == before:
            attempts += 1
            print(f"Still no data is returned, attempt Number: {attempts}")

        successful_sites = result_df["site_code"].unique()
        mask = ~df_failed_sites["site_code"].isin(successful_sites)
        failed_sites = df_failed_sites.loc[mask, "site_code"].tolist()

        db.update_tracking_status(successful_sites, status=True)
        db.update_tracking_status(failed_sites, status=False)


if __name__ == "__main__":
    create_meta_table()
    create_weather_data()
    create_tracker_table()

    new_sites_fetch_data(PATH)
