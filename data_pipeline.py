from datetime import datetime as dt
from datetime import timedelta as td
from retry_requests import retry

import openmeteo_requests
import pandas as pd
import requests_cache
import database as db
import time
import sys

# retries and backoff factors
retry_session = retry(retries=3, backoff_factor=0.5)
openmeteo = openmeteo_requests.Client(session=retry_session)

# lets define constant varaibles
URL = "https://api.open-meteo.com/v1/forecast"
PATH = "meta_data.csv"
START_TIME = dt.now()
TRACKING_PATH = "tracking.csv"


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


def parser(row):
    site_code = row["site_code"]

    # calcualting date based on current date
    end_date = (dt.now() + td(days=1)).strftime("%Y-%m-%d")
    diff = (dt.now() + td(days=1)) - td(days=8)
    start_date = diff.strftime("%Y-%m-%d")

    # parameters for request
    params = {
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "hourly": ["temperature_2m", "relative_humidity_2m", "shortwave_radiation"],
        "timezone": "auto",
        "start_date": start_date,
        "end_date": end_date,
    }

    try:
        # sending request to API
        responses = openmeteo.weather_api(url=URL, params=params)

    except Exception as e:
        error_message = str(e)

        # raise error if hourly limit reached
        if "Hourly API request limit exceeded" in error_message:
            time_elapsed = dt.now() - START_TIME
            remaining_time = td(hours=1) - time_elapsed
            wait_seconds = max(
                remaining_time.total_seconds(), 0
            )  # in case if Hourly limit error hit unexpectidly after an hour for some reason
            time.sleep(wait_seconds)
            print(f"Hourly API request limit exceeded: {wait_seconds}")

        elif "Minutely API request limit exceeded" in error_message:
            time_elapsed = dt.now() - START_TIME
            remaining_time = td(minutes=1) - time_elapsed
            wait_seconds = max(
                remaining_time.total_seconds(), 0
            )  # same reason as above
            time.sleep(wait_seconds)
            print(f"Minutely API request limit exceeded: {wait_seconds}")

        elif "Daily API request limit exceeded" in error_message:
            sys.exit("Daily API request limit reached, give it a rest see ya tomorrow")

        # response failed so status is going to be False
        db.update_tracking_status(site_code=site_code, status=False)
        return None

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

    # response did not failed so status is going to be True
    db.update_tracking_status(site_code=site_code, status=True)
    return hourly_dataframe


def new_sites_fetch_data(path):
    # changing the global variable here in case if we import in function in another file
    # and call the function there it will not reset the start time after the first execution.
    global START_TIME
    START_TIME = dt.now()

    # batch size
    batch_size = 100

    # inserting the meta data in database
    insert_meta_data(path)

    # fetching the data
    df = pd.read_csv(path)

    # instead of getting the data of 10,000 sites we are gonna insert 100 sites data over time
    for start in range(0, len(df), batch_size):

        batch_df = df.iloc[start : start + batch_size]
        df_series = batch_df.apply(parser, axis=1)

        # filtered the data as failed values returned None
        df_filtered = [data for data in df_series if data is not None]

        # a small check in case if the list is empty
        if not df_filtered:
            print("There is nothing to insert into database, Please try again!")
            return

        result_df = pd.concat(df_filtered)

        # inserting the data in database
        insert_weather_data(result_df)


def failed_sites_retry(path):
    # changing the global variable here in case if we import in function in another file
    # and call the function there it will not reset the start time after the first execution.
    attempts = 0
    while True:
        global START_TIME
        START_TIME = dt.now()

        # using tracker to filter sites that failed
        df = pd.read_csv(path)
        tracker = db.read_failed_sites()

        # filtering only sites that failed
        failed_sites = tracker.loc[tracker["status"] == False]

        # if there are no failed site break the loop
        if failed_sites.empty:
            break

        mask = df["site_code"].isin(failed_sites["site_code"])
        df_failed_sites = df.loc[mask]

        # now using dataframe of failed sites only we are going to retry them
        df_series = df_failed_sites.apply(parser, axis=1)

        df_filtered = [data for data in df_series if data is not None]

        if not df_filtered:
            attempts += 1
            print(f"Still no data is returned, attempt Number: {attempts}")
            continue

        result_df = pd.concat(df_filtered)

        # now we are going to insert the filtered data in database
        insert_weather_data(result_df)


if __name__ == "__main__":
    create_meta_table()
    create_weather_data()
    create_tracker_table()

    new_sites_fetch_data(PATH)
