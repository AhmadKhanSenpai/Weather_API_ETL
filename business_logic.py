from datetime import datetime as dt
from datetime import timedelta as td
from retry_requests import retry

import openmeteo_requests
import pandas as pd
import requests_cache
import database as db
import time

# cache_session, retries and backoff factors
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=2, backoff_factor=0.5)
openmeteo = openmeteo_requests.Client(session=retry_session)

# lets define constant varaibles
URL = "https://api.open-meteo.com/v1/forecast"
PATH = "meta_data.csv"


# just a class for raising custom built error
class APIRateLimitError:
    "Raised when the Open-Meteo hourly rate limit exceeded"

    pass


def create_meta_table():
    db.create_sites_table()


def create_weather_data():
    db.create_weather_table()


def insert_meta_data(path):
    df = pd.read_csv(path)
    db.insert_meta_data(df)


def insert_weather_data(df):
    db.insert_weather_data(df)


def parameter_builder(file_path):
    df = pd.read_csv(file_path)

    # batch_size for each request 1000 is the limit
    batch_size = 1000

    # calcualting date based on current date
    end_date = dt.now().strftime("%Y-%m-%d")
    diff = (dt.now() + td(days=1)) - td(days=7)
    start_date = diff.strftime("%Y-%m-%d")

    for start_index in range(0, len(df), batch_size):

        # now i have a chunk of dataframe that i can work with upto 1000 rows
        df_batch = df.iloc[start_index : start_index + batch_size]

        params = {
            "latitude": df_batch["latitude"].tolist(),
            "longitude": df_batch["longitude"].tolist(),
            "hourly": ["temperature_2m", "relative_humidity_2m", "shortwave_radiation"],
            "timezone": "auto",
            "start_date": start_date,
            "end_date": end_date,
        }

        site_codes = df_batch["site_code"].tolist()

        yield site_codes, params


def fetch_weather_data(site_code, params):
    attempts = 3
    for attempt in range(attempts):
        try:
            # parsing the info from response
            responses = openmeteo.weather_api(url=URL, params=params)
            response = responses[0]

            # Process hourly data. The order of variables needs to be the same as requested.
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
            hourly_global_tilted_irradiance_instant = hourly.Variables(
                2
            ).ValuesAsNumpy()

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

        except Exception as e:
            error_message = str(e)
            print(f"{site_code} failed")
            print(f"Attempt {attempt + 1} / {attempts}")
            print(f"reason of faliure {e}")

            # raise error if hourly limit reached
            if "Hourly API request limit exceeded" in error_message:
                raise APIRateLimitError("API hourly limit reached")

            # if request failed then retry that request after 5 sec
            if attempt < attempts - 1:
                print("retrying after 5 seconds \n")
                time.sleep(5)

            else:
                print(f"request still failed after {attempts} attempts")
                print(f"reason of faliure: {e}")
                return None


def run_weather_etl(path):

    insert_meta_data(path)
    count = 0

    try:
        for site_code, params in parameter_builder(path):
            count += 1
            df = fetch_weather_data(site_code, params)

            if df is not None:
                insert_weather_data(df)
                print(count)

    except APIRateLimitError as e:
        print(e)
        print("stopping ETL because rate limit reached")


# testing
if __name__ == "__main__":
    # just building the schema if it does not exist
    create_meta_table()
    create_weather_data()

    # intiating the data extraction
    start_time = time.time()
    run_weather_etl(path=PATH)
    end_time = time.time()

    total_time = end_time - start_time

    print(f"Total execution time: {total_time/60:.2f} minutes")
