from datetime import datetime as dt
from datetime import timedelta as td
from retry_requests import retry

import openmeteo_requests
import pandas as pd
import requests_cache
import database as db
import time

# retries and backoff factors can handle errors
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# lets define constant varaibles
URL = "https://api.open-meteo.com/v1/forecast"
PATH = "meta_data.csv"


def creating_meta_data(path):
    df = pd.read_csv(path)
    db.insert_meta_data(df)


def creating_weather_data(df):
    db.insert_weather_data(df)


def parameter_builder(file_path):
    """
    this function will build the parameter dict
    and site code
    """
    df = pd.read_csv(file_path)

    # calcualting date based on current date
    end_date = dt.now().strftime("%Y-%m-%d")
    diff = dt.now() - td(days=7)
    start_date = diff.strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        params = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "global_tilted_irradiance_instant",
            ],
            "timezone": "auto",
            "start_date": start_date,
            "end_date": end_date,
        }

        return row["site_code"], params


def fetch_weather_data(site_code, params):
    # creating necessary table if not exist
    db.create_sites_table()
    db.create_weather_table()

    # parsing the info from response
    responses = openmeteo.weather_api(url=URL, params=params)
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
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
    }

    hourly_data["site_code"] = site_code
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["global_tilted_irradiance_instant"] = (
        hourly_global_tilted_irradiance_instant
    )

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return hourly_dataframe


def run_weather_etl(path):
    creating_meta_data(path)

    for site_code, params in parameter_builder(path):
        df = fetch_weather_data(site_code, params)
        creating_weather_data(df)
        time.sleep(1)


# testing
if __name__ == "__main__":
    start_time = time.time()
    run_weather_etl(path=PATH)
    end_time = time.time()

    total_time = end_time - start_time

    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Total execution time: {total_time/60:.2f} minutes")
